package file

import (
	"context"
	"errors"
	"io/fs"
	"time"

	"gorm.io/gorm"
	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/quota"
)

func (s *Service) CancelDirectUpload(ctx context.Context, owner int, id string) (DirectUploadInfo, error) {
	if e := s.terminateDirect(ctx, owner, id, "canceled", ""); e != nil {
		return DirectUploadInfo{}, e
	}
	// Cleanup is persisted and retried by the regular cleanup task if COS is down.
	d, e := s.directOwned(ctx, owner, id)
	if e != nil {
		return DirectUploadInfo{}, e
	}
	if d.CleanupPending {
		_ = s.cleanDirect(ctx, d)
	}
	return s.GetDirectUpload(ctx, owner, id)
}
func (s *Service) terminateDirect(ctx context.Context, owner int, id, status, reason string) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		d, e := directLock(tx, owner, id)
		if e != nil {
			return e
		}
		if d.Status == "completed" {
			return ErrDirectConflict
		}
		if d.Status == "canceled" || d.Status == "expired" || d.Status == "failed" {
			return nil
		}
		// A completing operation must first be reconciled. Cancellation cannot delete
		// an object while a merge request may still be publishing it.
		if d.Status == "completing" || (d.Status == "initializing" && time.Now().Before(d.OperationUntil)) {
			return ErrDirectConflict
		}
		if e = quota.ReleaseReserved(tx, owner, d.ReservedBytes); e != nil {
			return e
		}
		return tx.Model(d).Updates(map[string]any{"status": status, "last_error": reason, "reserved_bytes": 0, "cleanup_pending": true}).Error
	})
}
func (s *Service) cleanDirect(ctx context.Context, d model.DirectUploadSession) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	st, m, e := s.directStore(d)
	if e != nil {
		return e
	}
	if d.UploadID != "" {
		if e = m.Abort(ctx, d.StorageKey, d.UploadID); e != nil {
			return e
		}
	}
	if e = st.Remove(ctx, d.StorageKey); e != nil && !errors.Is(e, fs.ErrNotExist) {
		return e
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		current, e := directLock(tx, d.OwnerID, d.ID)
		if e != nil {
			return e
		}
		if !current.CleanupPending {
			return nil
		}
		if current.Status != "canceled" && current.Status != "expired" && current.Status != "failed" {
			return ErrDirectConflict
		}
		// Once abort/remove succeeded, hand the placeholder to the existing GC.
		if e = tx.Model(&model.Entity{}).Where("id = ? AND status = ? AND ref_count = 0", d.EntityID, model.EntityStatusUploading).Update("status", model.EntityStatusPendingDelete).Error; e != nil {
			return e
		}
		return tx.Model(current).Update("cleanup_pending", false).Error
	})
}

// ReconcileDirectUploads is bounded, persistent and safe across concurrent API
// cleanup workers. Uploading sessions are left intact across process restarts.
func (s *Service) ReconcileDirectUploads(ctx context.Context, limit int) error {
	if limit <= 0 {
		limit = 100
	}
	now := time.Now()
	var rows []model.DirectUploadSession
	e := s.db.WithContext(ctx).Where("cleanup_pending = ? OR status IN ? OR (status = ? AND expires_at <= ?)", true, []string{"initializing", "completing", "verifying"}, "uploading", now).Order("updated_at ASC, id ASC").Limit(limit).Find(&rows).Error
	if e != nil {
		return e
	}
	var failures []error
	for _, d := range rows {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		switch {
		case d.CleanupPending:
			e = s.cleanDirect(ctx, d)
		case d.Status == "initializing":
			if now.Before(d.OperationUntil) {
				continue
			}
			e = s.terminateDirect(ctx, d.OwnerID, d.ID, "failed", "Initialization result unknown; retry with a new request after lifecycle cleanup")
		case d.Status == "completing":
			if now.Before(d.OperationUntil) {
				continue
			}
			claim := s.db.WithContext(ctx).Model(&model.DirectUploadSession{}).Where("id = ? AND status = ? AND operation_until <= ?", d.ID, "completing", now).Update("operation_until", now.Add(2*time.Minute))
			if claim.Error != nil {
				e = claim.Error
				break
			}
			if claim.RowsAffected == 0 {
				continue
			}
			e = s.mergeDirect(ctx, d)
			if errors.Is(e, fs.ErrNotExist) {
				// Both unique object and multipart are absent. No merge can now publish it.
				e = s.db.WithContext(ctx).Model(&model.DirectUploadSession{}).Where("id = ? AND status = ?", d.ID, "completing").Update("status", "uploading").Error
				if e == nil {
					e = s.terminateDirect(ctx, d.OwnerID, d.ID, "failed", "Multipart and object are missing")
				}
			}
		case !now.Before(d.ExpiresAt):
			e = s.terminateDirect(ctx, d.OwnerID, d.ID, "expired", "Upload expired")
		case d.Status == "verifying":
			var job model.Task
			e = s.db.WithContext(ctx).Where("id = ?", d.VerifyTaskID).First(&job).Error
			if errors.Is(e, gorm.ErrRecordNotFound) {
				e = s.db.WithContext(ctx).Create(&model.Task{ID: d.VerifyTaskID, OwnerID: d.OwnerID, Type: DirectVerifyTask, Status: model.TaskStatusQueued, StateVersion: 1, StateJSON: `{"session_id":"` + d.ID + `"}`, MaxAttempts: 3, NextRunAt: now}).Error
			} else if e == nil && job.Status == model.TaskStatusFailed {
				e = s.terminateDirect(ctx, d.OwnerID, d.ID, "failed", job.LastError)
			}
		}
		// Rotate scanned records, so persistent cloud failures cannot starve later rows.
		updates := map[string]any{"updated_at": time.Now()}
		if e != nil {
			updates["last_error"] = e.Error()
			failures = append(failures, e)
		}
		if err := s.db.WithContext(ctx).Model(&model.DirectUploadSession{}).Where("id = ?", d.ID).Updates(updates).Error; err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}
