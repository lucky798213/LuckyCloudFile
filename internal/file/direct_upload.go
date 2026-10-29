package file

import (
	"context"
	"errors"
	"io/fs"
	"sort"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"luckyCloudFile/internal/idgen"
	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/quota"
	"luckyCloudFile/internal/storage"
)

const DirectPartSize int64 = 8 << 20

var ErrDirectUnavailable = errors.New("current storage policy does not support direct uploads")
var ErrDirectConflict = errors.New("direct upload state does not allow this operation")

type DirectUploadInfo struct {
	model.DirectUploadSession
	UploadedParts []storage.UploadedPart `json:"uploaded_parts"`
}

func (s *Service) directStore(d model.DirectUploadSession) (storage.Store, storage.MultipartStore, error) {
	st, e := s.stores.Get(d.StoragePolicyID)
	if e != nil {
		return nil, nil, e
	}
	m, ok := st.(storage.MultipartStore)
	if !ok {
		return nil, nil, ErrDirectUnavailable
	}
	return st, m, nil
}
func (s *Service) directOwned(ctx context.Context, owner int, id string) (model.DirectUploadSession, error) {
	var d model.DirectUploadSession
	e := s.db.WithContext(ctx).Where("id = ? AND owner_id = ?", id, owner).First(&d).Error
	if errors.Is(e, gorm.ErrRecordNotFound) {
		e = ErrNotFound
	}
	return d, e
}
func directLock(tx *gorm.DB, owner int, id string) (*model.DirectUploadSession, error) {
	if _, e := quota.LockUser(tx, owner); e != nil {
		return nil, e
	}
	var d model.DirectUploadSession
	e := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ? AND owner_id = ?", id, owner).First(&d).Error
	if errors.Is(e, gorm.ErrRecordNotFound) {
		e = ErrNotFound
	}
	return &d, e
}
func (s *Service) CreateDirectUpload(ctx context.Context, owner int, parent, name string, size int64, digest, requestID string) (DirectUploadInfo, error) {
	if e := validateName(name); e != nil {
		return DirectUploadInfo{}, e
	}
	if size <= 0 {
		return DirectUploadInfo{}, ErrInvalidSize
	}
	if size > s.directMax {
		return DirectUploadInfo{}, ErrTooLarge
	}
	digest, e := normalizeContentSHA256(digest)
	if e != nil {
		return DirectUploadInfo{}, e
	}
	if requestID == "" || len(requestID) > 255 {
		return DirectUploadInfo{}, ErrInvalidIdempotencyKey
	}
	requestDigest := instantRequestDigest(owner, parent, name, size, digest)
	policy, st, e := s.uploadStore(ctx, owner)
	if e != nil {
		return DirectUploadInfo{}, e
	}
	m, ok := st.(storage.MultipartStore)
	if !ok {
		return DirectUploadInfo{}, ErrDirectUnavailable
	}
	id, e := idgen.New()
	if e != nil {
		return DirectUploadInfo{}, e
	}
	d := model.DirectUploadSession{ID: id, OwnerID: owner, RequestID: requestID, RequestDigest: requestDigest, Name: name, ExpectedSize: size, ExpectedSHA256: digest, PartSize: DirectPartSize, ReservedBytes: size, StoragePolicyID: policy, StorageKey: "direct-" + id, Status: "initializing", ExpiresAt: time.Now().Add(24 * time.Hour), OperationUntil: time.Now().Add(2 * time.Minute)}
	created := false
	e = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if _, e := quota.LockUser(tx, owner); e != nil {
			return e
		}
		var old model.DirectUploadSession
		e := tx.Where("owner_id = ? AND request_id = ?", owner, requestID).First(&old).Error
		if e == nil {
			if old.RequestDigest != requestDigest {
				return ErrIdempotencyConflict
			}
			d = old
			return nil
		}
		if !errors.Is(e, gorm.ErrRecordNotFound) {
			return e
		}
		dir, e := queryDirectory(ctx, tx, owner, parent)
		if e != nil {
			return e
		}
		d.ParentID = dir.ID
		if e = quota.Reserve(tx, owner, size); e != nil {
			return e
		}
		entity := model.Entity{StorageKey: d.StorageKey, StoragePolicyID: policy, Status: model.EntityStatusUploading}
		if e = tx.Create(&entity).Error; e != nil {
			return e
		}
		d.EntityID = entity.ID
		if e = tx.Create(&d).Error; e != nil {
			return e
		}
		created = true
		return nil
	})
	if e != nil {
		return DirectUploadInfo{}, e
	}
	if created {
		callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		uploadID, err := m.Initiate(callCtx, d.StorageKey)
		if err != nil {
			return DirectUploadInfo{}, err
		} // Unknown init is retained, never blindly retried; bucket lifecycle catches orphan multipart.
		result := s.db.WithContext(ctx).Model(&model.DirectUploadSession{}).Where("id = ? AND status = ?", id, "initializing").Updates(map[string]any{"upload_id": uploadID, "status": "uploading"})
		if result.Error != nil {
			return DirectUploadInfo{}, result.Error
		}
		d.UploadID = uploadID
		d.Status = "uploading"
	}
	return DirectUploadInfo{d, []storage.UploadedPart{}}, nil
}
func (s *Service) GetDirectUpload(ctx context.Context, owner int, id string) (DirectUploadInfo, error) {
	d, e := s.directOwned(ctx, owner, id)
	if e != nil {
		return DirectUploadInfo{}, e
	}
	info := DirectUploadInfo{d, []storage.UploadedPart{}}
	if d.Status == "uploading" {
		_, m, e := s.directStore(d)
		if e != nil {
			return info, e
		}
		info.UploadedParts, e = m.ListParts(ctx, d.StorageKey, d.UploadID)
		if e != nil {
			return info, e
		}
	}
	return info, nil
}
func (s *Service) SignDirectParts(ctx context.Context, owner int, id string, numbers []int) (map[int]storage.SignedPart, error) {
	d, e := s.directOwned(ctx, owner, id)
	if e != nil {
		return nil, e
	}
	if d.Status != "uploading" || !time.Now().Before(d.ExpiresAt) {
		return nil, ErrDirectConflict
	}
	if len(numbers) == 0 || len(numbers) > 16 {
		return nil, ErrInvalidChunkIndex
	}
	_, m, e := s.directStore(d)
	if e != nil {
		return nil, e
	}
	out := map[int]storage.SignedPart{}
	for _, n := range numbers {
		if n < 1 || int64(n) > (d.ExpectedSize+d.PartSize-1)/d.PartSize {
			return nil, ErrInvalidChunkIndex
		}
		signed, e := m.SignPart(ctx, d.StorageKey, d.UploadID, n, 5*time.Minute)
		if e != nil {
			return nil, e
		}
		out[n] = signed
	}
	return out, nil
}
func (s *Service) CompleteDirectUpload(ctx context.Context, owner int, id string) (DirectUploadInfo, error) {
	d, e := s.directOwned(ctx, owner, id)
	if e != nil {
		return DirectUploadInfo{}, e
	}
	if d.Status == "completed" || d.Status == "verifying" {
		return DirectUploadInfo{d, []storage.UploadedPart{}}, nil
	}
	result := s.db.WithContext(ctx).Model(&model.DirectUploadSession{}).Where("id = ? AND owner_id = ? AND status = ? AND expires_at > ?", id, owner, "uploading", time.Now()).Updates(map[string]any{"status": "completing", "operation_until": time.Now().Add(2 * time.Minute)})
	if result.Error != nil {
		return DirectUploadInfo{}, result.Error
	}
	if result.RowsAffected != 1 {
		return DirectUploadInfo{}, ErrDirectConflict
	}
	d.Status = "completing"
	if e = s.mergeDirect(ctx, d); e != nil {
		return DirectUploadInfo{}, e
	}
	return s.GetDirectUpload(ctx, owner, id)
}
func (s *Service) mergeDirect(ctx context.Context, d model.DirectUploadSession) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	st, m, e := s.directStore(d)
	if e != nil {
		return e
	}
	// An object at this unique key proves an earlier complete succeeded. The worker
	// still checks every byte; never trust the multipart ETag as a content digest.
	r, e := st.Open(ctx, d.StorageKey)
	if e == nil {
		r.Close()
		return s.enqueueDirectVerify(ctx, d)
	}
	if !errors.Is(e, fs.ErrNotExist) {
		return e
	}
	parts, e := m.ListParts(ctx, d.StorageKey, d.UploadID)
	if e != nil {
		return e
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].Number < parts[j].Number })
	count := int((d.ExpectedSize + d.PartSize - 1) / d.PartSize)
	valid := len(parts) == count
	for i, p := range parts {
		want := d.PartSize
		if i == count-1 {
			want = d.ExpectedSize - int64(i)*d.PartSize
		}
		if p.Number != i+1 || p.Size != want || p.ETag == "" {
			valid = false
		}
	}
	if !valid {
		// No merge was sent: it is safe to let the client fill missing/wrong parts.
		s.db.WithContext(ctx).Model(&model.DirectUploadSession{}).Where("id = ? AND status = ?", d.ID, "completing").Update("status", "uploading")
		return ErrDirectConflict
	}
	if e = m.Complete(ctx, d.StorageKey, d.UploadID, parts); e != nil {
		return e
	} // Keep completing on unknown result; recovery checks object first.
	return s.enqueueDirectVerify(ctx, d)
}

const DirectVerifyTask = "direct_upload_verify_v1"

func (s *Service) enqueueDirectVerify(ctx context.Context, d model.DirectUploadSession) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		current, e := directLock(tx, d.OwnerID, d.ID)
		if e != nil {
			return e
		}
		if current.Status == "verifying" || current.Status == "completed" {
			return nil
		}
		if current.Status != "completing" {
			return ErrDirectConflict
		}
		taskID, e := idgen.New()
		if e != nil {
			return e
		}
		job := model.Task{ID: taskID, OwnerID: d.OwnerID, Type: DirectVerifyTask, Status: model.TaskStatusQueued, StateVersion: 1, StateJSON: `{"session_id":"` + d.ID + `"}`, MaxAttempts: 3, NextRunAt: time.Now()}
		if e = tx.Create(&job).Error; e != nil {
			return e
		}
		return tx.Model(current).Updates(map[string]any{"status": "verifying", "verify_task_id": taskID}).Error
	})
}

func (s *Service) DirectCapabilities(ctx context.Context, owner int) (bool, int64, error) {
	_, st, e := s.uploadStore(ctx, owner)
	if e != nil {
		return false, 0, e
	}
	_, enabled := st.(storage.MultipartStore)
	if enabled {
		return true, s.directMax, nil
	}
	return false, MaxUploadBytes, nil
}
