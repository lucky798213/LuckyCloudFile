package archive

import (
	"context"
	"fmt"
	"io"

	"luckyCloudFile/internal/file"
)

// SnapshotSources 记录 fake 来源文件的当前身份。
func (f *fakeService) SnapshotSources(
	_ context.Context,
	ownerID int,
	ids []string,
) ([]file.SourceSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	snapshots := make([]file.SourceSnapshot, 0, len(ids))
	for _, id := range ids {
		source, ok := f.sources[id]
		if !ok || source.owner != ownerID {
			return nil, file.ErrNotFound
		}
		snapshots = append(snapshots, file.SourceSnapshot{
			FileID:   id,
			EntityID: source.entityID,
			ParentID: source.parentID,
			Name:     source.name,
			Size:     int64(len(source.data)),
		})
	}
	return snapshots, nil
}

// ProtectSources 模拟“重新取得保护并核对快照”。
func (f *fakeService) ProtectSources(
	_ context.Context,
	ownerID int,
	snapshots []file.SourceSnapshot,
) (file.SourceGuard, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.protectCalls++
	for _, snapshot := range snapshots {
		source, ok := f.sources[snapshot.FileID]
		if !ok || source.owner != ownerID {
			return nil, file.ErrSourceChanged
		}
		if source.entityID != snapshot.EntityID ||
			source.parentID != snapshot.ParentID ||
			source.name != snapshot.Name ||
			int64(len(source.data)) != snapshot.Size {
			return nil, file.ErrSourceChanged
		}
	}
	return &fakeSourceGuard{
		service:   f,
		ownerID:   ownerID,
		snapshots: append([]file.SourceSnapshot(nil), snapshots...),
	}, nil
}

// ValidateTarget 模拟目标校验；parentID 为空表示根目录。
func (f *fakeService) ValidateTarget(
	_ context.Context,
	_ int,
	parentID, _ string,
) (string, error) {
	if f.validateErr != nil {
		return "", f.validateErr
	}
	return parentID, nil
}

// FindTaskResult 返回 fake 里已经发布的任务结果。
func (f *fakeService) FindTaskResult(
	_ context.Context,
	ownerID int,
	taskID string,
) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	result, ok := f.taskResults[taskID]
	if !ok || result.owner != ownerID {
		return "", file.ErrNotFound
	}
	return result.fileID, nil
}

type fakeSourceGuard struct {
	service   *fakeService
	ownerID   int
	snapshots []file.SourceSnapshot
	released  bool
}

func (g *fakeSourceGuard) Open(
	_ context.Context,
	index int,
) (file.SourceSnapshot, io.ReadCloser, error) {
	if g.released {
		return file.SourceSnapshot{}, nil, fmt.Errorf("fake source guard released")
	}
	if index < 0 || index >= len(g.snapshots) {
		return file.SourceSnapshot{}, nil, fmt.Errorf("source index %d out of range", index)
	}

	snapshot := g.snapshots[index]
	g.service.mu.Lock()
	source, ok := g.service.sources[snapshot.FileID]
	g.service.mu.Unlock()

	if !ok || source.owner != g.ownerID ||
		source.entityID != snapshot.EntityID ||
		source.parentID != snapshot.ParentID ||
		source.name != snapshot.Name ||
		int64(len(source.data)) != snapshot.Size {
		return file.SourceSnapshot{}, nil, file.ErrSourceChanged
	}
	if source.reader != nil {
		return snapshot, source.reader, nil
	}
	return snapshot, &fakeReader{
		service: g.service,
		id:      snapshot.FileID,
		data:    source.data,
		readErr: source.readErr,
	}, nil
}

func (g *fakeSourceGuard) Release() {
	if g.released {
		return
	}
	g.released = true
	g.service.mu.Lock()
	g.service.releases++
	g.service.mu.Unlock()
}
