package file

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/pathlock"
)

// uploadLockBinding 是上传会话和它占用的目标路径锁之间的绑定。
//
// 会话创建时申请一把目标节点锁，把 token 留在这里；之后每次传输和发布
// 都用同一个 token 进入短时保护，不会重新申请一把新锁。令牌失效或绑定
// 不存在时请求必须失败，不能悄悄拿一把新锁让旧会话恢复写入。
//
// token 是服务端内部协调信息：只由服务端生成和保存，不接受客户端指定，
// 也不通过任何 API 返回。本阶段不提供前端加锁/解锁接口。
type uploadLockBinding struct {
	namespace string
	path      string
	token     string
}

// uploadLockRegistry 是 sessionID → 资源锁绑定的进程内登记表。
//
// 和分片账本一样只存在于当前进程：服务重启时未完成会话统一失效，
// 不恢复旧令牌，也不承诺跨进程续传。
type uploadLockRegistry struct {
	mu        sync.Mutex
	bySession map[string]uploadLockBinding
}

func newUploadLockRegistry() *uploadLockRegistry {
	return &uploadLockRegistry{bySession: make(map[string]uploadLockBinding)}
}

// bind 建立 sessionID 和一把已经持有的资源锁的绑定。
//
// 会话创建时必须在数据库事务提交之前调用：这样会话一旦可见，
// 绑定已经准备好了，不会出现“会话已可见但绑定尚未准备好”的窗口。
func (r *uploadLockRegistry) bind(sessionID string, lease pathlock.Lease) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.bySession[sessionID] = uploadLockBinding{
		namespace: lease.Request.Namespace,
		path:      lease.Request.Path,
		token:     lease.Token,
	}
}

func (r *uploadLockRegistry) lookup(sessionID string) (uploadLockBinding, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	binding, ok := r.bySession[sessionID]
	return binding, ok
}

// remove 删除绑定并返回它，供调用方真正 Unlock 使用。
func (r *uploadLockRegistry) remove(sessionID string) (uploadLockBinding, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	binding, ok := r.bySession[sessionID]
	if ok {
		delete(r.bySession, sessionID)
	}
	return binding, ok
}

// confirmUploadSessionLock 用绑定令牌进入本次请求的短时保护。
//
// 返回的函数只释放本次保护，不会释放整个会话锁。绑定不存在、令牌已经
// 失效或正在关闭时拒绝请求；调用方不能再申请新锁。
func (s *Service) confirmUploadSessionLock(sessionID string) (func(), error) {
	binding, ok := s.uploadLocks.lookup(sessionID)
	if !ok {
		return nil, fmt.Errorf(
			"%w: resource lock binding for session %s is missing",
			ErrUploadSessionConflict, sessionID,
		)
	}

	release, err := s.pathLocks.Confirm(
		binding.namespace,
		binding.path,
		binding.token,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"%w: confirm resource lock for session %s: %v",
			ErrUploadSessionConflict, sessionID, err,
		)
	}
	return release, nil
}

// closeUploadSessionLock 关闭会话资源锁：先删除绑定，再 Unlock。
//
// Unlock 只标记关闭并拒绝新的 Confirm；已经有保护区在运行时，锁会等
// 最后一个保护区退出后才真正从管理器移除，所以这里不会等待传输请求。
// 重复关闭是幂等的。
func (s *Service) closeUploadSessionLock(sessionID string) {
	binding, ok := s.uploadLocks.remove(sessionID)
	if !ok {
		return
	}

	err := s.pathLocks.Unlock(binding.namespace, binding.token)
	if err != nil && !errors.Is(err, pathlock.ErrNoSuchLock) {
		s.logger.Warn("close upload session resource lock",
			"session_id", sessionID,
			"path", binding.path,
			"error", err,
		)
	}
}

// findUploadSessionForVerification 用独立、有超时的短事务核对会话是否存在。
//
// 不能使用普通 SELECT：原创建事务可能还在提交，普通查询暂时看不到未提交的
// 会话，会把“正在提交”误判成“确定不存在”，随后撤销绑定并解锁，留下
// 有会话、无资源锁的状态。
//
// 这里在独立事务里用 SELECT ... FOR UPDATE 按主键核对，会等待原事务的
// 行锁/插入锁结束：
//   - 原事务提交：锁定读看到会话，返回 (session, true, nil)；
//   - 原事务回滚：锁定读看不到会话，返回 (nil, false, nil)；
//   - 等待超时或查询失败：返回错误，调用方必须按“不确定”保留锁。
func (s *Service) findUploadSessionForVerification(
	ctx context.Context,
	ownerID int,
	sessionID string,
) (*model.UploadSession, bool, error) {
	verifyCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), orphanCleanupTimeout)
	defer cancel()

	var (
		session model.UploadSession
		found   bool
	)
	err := s.db.WithContext(verifyCtx).Transaction(func(tx *gorm.DB) error {
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND owner_id = ?", sessionID, ownerID).
			First(&session).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return fmt.Errorf("lock upload session for creation verification: %w", err)
		}
		found = true
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}
	return &session, true, nil
}

// settleFailedUploadSessionCreation 在创建事务返回错误后核对提交结果，
// 并据此处理创建时申请的锁和绑定：
//   - 会话已经提交：保留锁和绑定，返回会话；
//   - 确定没有提交：删除绑定并释放锁；
//   - 无法核对：保留锁和绑定，返回错误。
//
// 调用方在自己的 defer 里仍会执行一次清理；这里的释放是幂等的，
// 所以确定回滚时不会重复 Unlock。
func (s *Service) settleFailedUploadSessionCreation(
	ctx context.Context,
	ownerID int,
	sessionID string,
	guard *pathLockGuard,
) (*model.UploadSession, bool, error) {
	verified, found, err := s.findUploadSessionForVerification(ctx, ownerID, sessionID)
	if err != nil {
		return nil, true, err
	}
	if !found {
		s.uploadLocks.remove(sessionID)
		guard.release()
		return nil, false, nil
	}
	return verified, true, nil
}
