package file

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"luckyCloudFile/internal/database"
	"luckyCloudFile/internal/idgen"
	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/queue"
	"luckyCloudFile/internal/quota"
	"luckyCloudFile/internal/task"
)

func (s *Service) DirectVerifyFactory(tasks *task.Service) task.Factory {
	return task.FactoryFunc(func(record model.Task) (task.Runner, error) {
		var state struct {
			SessionID string `json:"session_id"`
		}
		if record.Type != DirectVerifyTask || record.StateVersion != 1 || json.Unmarshal([]byte(record.StateJSON), &state) != nil || state.SessionID == "" || record.LeaseUntil == nil {
			return nil, errors.New("invalid direct verify task")
		}
		claim := task.Claim{TaskID: record.ID, WorkerID: record.WorkerID, Attempt: record.Attempts, LeaseUntil: *record.LeaseUntil, Record: record}
		return task.RunnerFunc(func(ctx context.Context, _ model.Task, _ queue.ProgressFunc) (string, error) {
			d, e := s.directOwned(ctx, record.OwnerID, state.SessionID)
			if e != nil {
				return "", e
			}
			if d.Status != "verifying" || d.VerifyTaskID != record.ID {
				return "", ErrDirectConflict
			}
			st, _, e := s.directStore(d)
			if e != nil {
				return "", e
			}
			r, e := st.Open(ctx, d.StorageKey)
			if e != nil {
				return "", task.Retryable(e)
			}
			h := sha256.New()
			n, e := io.Copy(h, io.LimitReader(r, d.ExpectedSize+1))
			r.Close()
			if e != nil {
				return "", task.Retryable(e)
			}
			digest := hex.EncodeToString(h.Sum(nil))
			if n != d.ExpectedSize || digest != d.ExpectedSHA256 {
				return "", errors.New("direct upload size or SHA-256 mismatch")
			}
			fileID, e := idgen.New()
			if e != nil {
				return "", e
			}
			result, _ := json.Marshal(map[string]string{"file_id": fileID})
			for attempt := 0; attempt < contentPublishAttempts; attempt++ {
				e = tasks.Complete(ctx, claim, string(result), func(tx *gorm.DB) error { return s.publishDirect(tx, d, fileID) })
				if e == nil {
					return fileID, nil
				}
				if !errors.Is(e, errContentKeyRace) && !database.IsRetryableConflict(e) {
					return "", e
				}
			}
			return "", task.Retryable(e)
		}), nil
	})
}
func (s *Service) publishDirect(tx *gorm.DB, d model.DirectUploadSession, fileID string) error {
	current, e := directLock(tx, d.OwnerID, d.ID)
	if e != nil {
		return e
	}
	if current.Status != "verifying" || current.VerifyTaskID != d.VerifyTaskID || !time.Now().Before(current.ExpiresAt) {
		return ErrDirectConflict
	}
	// Lock the actual parent, so deletion cannot race a new child insertion.
	var parent model.File
	if e = tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ? AND owner_id = ? AND kind = ?", d.ParentID, d.OwnerID, model.KindDirectory).First(&parent).Error; e != nil {
		return e
	}
	entity, e := lockEntity(tx.Statement.Context, tx, d.EntityID)
	if e != nil {
		return e
	}
	if entity.Status != model.EntityStatusUploading || entity.RefCount != 0 {
		return ErrDirectConflict
	}
	reused, e := reuseExistingContent(tx, d.OwnerID, d.ParentID, fileID, d.Name, d.StoragePolicyID, d.ExpectedSHA256, d.ExpectedSize)
	if e != nil {
		return e
	}
	if reused {
		e = tx.Model(entity).Update("status", model.EntityStatusPendingDelete).Error
	} else {
		f := model.File{ID: fileID, Name: d.Name, Kind: model.KindFile, OwnerID: d.OwnerID, ParentID: &d.ParentID, EntityID: &d.EntityID}
		if e = tx.Create(&f).Error; e != nil {
			return e
		}
		if e = tx.Model(entity).Updates(map[string]any{"status": model.EntityStatusActive, "ref_count": 1, "size": d.ExpectedSize, "content_sha256": d.ExpectedSHA256}).Error; e != nil {
			return e
		}
		e = insertContentKey(tx, d.OwnerID, d.StoragePolicyID, d.ExpectedSHA256, d.ExpectedSize, d.EntityID)
	}
	if e != nil {
		return e
	}
	if e = quota.Settle(tx, d.OwnerID, current.ReservedBytes); e != nil {
		return e
	}
	return tx.Model(current).Updates(map[string]any{"status": "completed", "reserved_bytes": 0, "result_file_id": fileID}).Error
}
