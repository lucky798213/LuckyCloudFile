package model

import "time"

// Direct uploads are intentionally independent of process-local proxy sessions.
type DirectUploadSession struct {
	ID              string    `gorm:"type:varchar(64);primaryKey" json:"id"`
	OwnerID         int       `gorm:"not null;uniqueIndex:idx_direct_request,priority:1" json:"-"`
	RequestID       string    `gorm:"type:varchar(255);not null;uniqueIndex:idx_direct_request,priority:2" json:"-"`
	RequestDigest   string    `gorm:"type:char(64);not null" json:"-"`
	ParentID        string    `gorm:"type:varchar(255);not null" json:"parent_id"`
	Name            string    `gorm:"type:varchar(255);not null" json:"name"`
	EntityID        int64     `gorm:"not null" json:"-"`
	StoragePolicyID int       `gorm:"not null" json:"-"`
	StorageKey      string    `gorm:"type:varchar(255);not null" json:"-"`
	UploadID        string    `gorm:"type:varchar(1024);not null" json:"-"`
	ExpectedSize    int64     `gorm:"not null" json:"expected_size"`
	ExpectedSHA256  string    `gorm:"type:char(64);not null" json:"-"`
	PartSize        int64     `gorm:"not null" json:"part_size"`
	ReservedBytes   int64     `gorm:"not null" json:"-"`
	Status          string    `gorm:"type:varchar(24);not null;index" json:"status"`
	ExpiresAt       time.Time `gorm:"not null;index" json:"expires_at"`
	OperationUntil  time.Time `json:"-"`
	VerifyTaskID    string    `gorm:"type:varchar(64)" json:"verify_task_id,omitempty"`
	ResultFileID    string    `gorm:"type:varchar(255)" json:"result_file_id,omitempty"`
	LastError       string    `gorm:"type:text" json:"last_error,omitempty"`
	CleanupPending  bool      `gorm:"not null;index" json:"-"`
	CreatedAt       time.Time `json:"-"`
	UpdatedAt       time.Time `json:"-"`
}
