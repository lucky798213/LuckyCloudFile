package file

import (
	"context"
	"errors"
	"strings"
	"testing"

	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/pathlock"
)

func TestResolvePathBuildsVirtualPath(t *testing.T) {
	service, _, _, ownerA, ownerB := newTestService(t)
	ctx := context.Background()

	docs, err := service.CreateDirectory(ctx, ownerA.ID, "", "docs")
	if err != nil {
		t.Fatal(err)
	}
	notes, err := service.CreateDirectory(ctx, ownerA.ID, docs.ID, "notes")
	if err != nil {
		t.Fatal(err)
	}
	report, err := service.Upload(ctx, ownerA.ID, notes.ID, "report.txt", strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}

	snapshot, err := service.resolvePath(ctx, ownerA.ID, report.ID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Namespace != pathlock.Namespace(ownerA.ID) {
		t.Fatalf("namespace = %q, want %q", snapshot.Namespace, pathlock.Namespace(ownerA.ID))
	}
	if snapshot.Path != "/docs/notes/report.txt" {
		t.Fatalf("path = %q, want /docs/notes/report.txt", snapshot.Path)
	}

	names := make([]string, 0, len(snapshot.Ancestors))
	for _, node := range snapshot.Ancestors {
		names = append(names, node.Name)
	}
	wantNames := []string{"/", "docs", "notes", "report.txt"}
	if strings.Join(names, ",") != strings.Join(wantNames, ",") {
		t.Fatalf("ancestor names = %v, want %v", names, wantNames)
	}
	if snapshot.Ancestors[len(snapshot.Ancestors)-1].ID != report.ID {
		t.Fatalf("last snapshot node = %s, want target %s", snapshot.Ancestors[len(snapshot.Ancestors)-1].ID, report.ID)
	}
	if snapshot.Ancestors[0].ID != *ownerA.RootFileID {
		t.Fatalf("first snapshot node = %s, want root %s", snapshot.Ancestors[0].ID, *ownerA.RootFileID)
	}

	// 空 ID 解析到用户根目录。
	root, err := service.resolvePath(ctx, ownerA.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	if root.Path != "/" || len(root.Ancestors) != 1 || root.Ancestors[0].ID != *ownerA.RootFileID {
		t.Fatalf("unexpected root snapshot: %+v", root)
	}

	// 新建目标还不存在时，路径包含新名字，快照只包含已存在的父目录链。
	target, err := service.resolveTargetPath(ctx, ownerA.ID, notes.ID, "new.txt")
	if err != nil {
		t.Fatal(err)
	}
	if target.Path != "/docs/notes/new.txt" {
		t.Fatalf("target path = %q, want /docs/notes/new.txt", target.Path)
	}
	if len(target.Ancestors) != 3 {
		t.Fatalf("target ancestors = %d, want 3 existing nodes", len(target.Ancestors))
	}
	if target.Ancestors[len(target.Ancestors)-1].ID != notes.ID {
		t.Fatalf("target last node = %s, want parent %s", target.Ancestors[len(target.Ancestors)-1].ID, notes.ID)
	}

	if _, err := service.resolvePath(ctx, ownerB.ID, report.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("resolve another user's node error = %v, want ErrNotFound", err)
	}
	if _, err := service.resolveTargetPath(ctx, ownerA.ID, notes.ID, "bad/name"); !errors.Is(err, ErrInvalidName) {
		t.Fatalf("invalid target name error = %v, want ErrInvalidName", err)
	}
	if _, err := service.resolveTargetPath(ctx, ownerA.ID, report.ID, "child.txt"); !errors.Is(err, ErrNotDirectory) {
		t.Fatalf("file parent error = %v, want ErrNotDirectory", err)
	}
}

func TestVerifyPathSnapshotDetectsChanges(t *testing.T) {
	service, _, _, ownerA, _ := newTestService(t)
	ctx := context.Background()

	docs, err := service.CreateDirectory(ctx, ownerA.ID, "", "docs")
	if err != nil {
		t.Fatal(err)
	}
	report, err := service.Upload(ctx, ownerA.ID, docs.ID, "report.txt", strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}

	snapshot, err := service.resolvePath(ctx, ownerA.ID, report.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.verifyPathSnapshot(ctx, snapshot); err != nil {
		t.Fatalf("fresh snapshot should verify: %v", err)
	}

	// 祖先改名：字段比较必须发现。
	if _, err := service.Rename(ctx, ownerA.ID, docs.ID, "archive"); err != nil {
		t.Fatal(err)
	}
	if err := service.verifyPathSnapshot(ctx, snapshot); !errors.Is(err, ErrResourceChanged) {
		t.Fatalf("renamed ancestor error = %v, want ErrResourceChanged", err)
	}

	// 重新解析后快照重新有效。
	snapshot, err = service.resolvePath(ctx, ownerA.ID, report.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.verifyPathSnapshot(ctx, snapshot); err != nil {
		t.Fatalf("re-resolved snapshot should verify: %v", err)
	}

	// 目标被删除：查回来的节点少了，也必须是资源已变化。
	if err := service.Delete(ctx, ownerA.ID, report.ID); err != nil {
		t.Fatal(err)
	}
	if err := service.verifyPathSnapshot(ctx, snapshot); !errors.Is(err, ErrResourceChanged) {
		t.Fatalf("deleted target error = %v, want ErrResourceChanged", err)
	}
}

func TestResolvePathRejectsBrokenTree(t *testing.T) {
	service, db, _, ownerA, _ := newTestService(t)
	ctx := context.Background()

	a, err := service.CreateDirectory(ctx, ownerA.ID, "", "a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := service.CreateDirectory(ctx, ownerA.ID, "", "b")
	if err != nil {
		t.Fatal(err)
	}

	// a → b → a 形成环。
	if err := db.WithContext(ctx).Model(&model.File{}).
		Where("id = ? AND owner_id = ?", a.ID, ownerA.ID).
		Update("parent_id", b.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.WithContext(ctx).Model(&model.File{}).
		Where("id = ? AND owner_id = ?", b.ID, ownerA.ID).
		Update("parent_id", a.ID).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := service.resolvePath(ctx, ownerA.ID, a.ID); err == nil {
		t.Fatal("resolving a cyclic tree must fail")
	}

	// 断开环，但让 b 变成一个不在根目录下的孤立节点。
	if err := db.WithContext(ctx).Model(&model.File{}).
		Where("id = ? AND owner_id = ?", b.ID, ownerA.ID).
		Update("parent_id", nil).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := service.resolvePath(ctx, ownerA.ID, a.ID); err == nil {
		t.Fatal("resolving an orphaned branch must fail")
	}

	// 祖先不是目录：在普通文件下面伪造一个子节点。
	plain, err := service.Upload(ctx, ownerA.ID, "", "plain.txt", strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	child := model.File{
		ID:       "child-under-file",
		Name:     "child",
		Kind:     model.KindDirectory,
		ParentID: &plain.ID,
		OwnerID:  ownerA.ID,
	}
	if err := db.WithContext(ctx).Create(&child).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := service.resolvePath(ctx, ownerA.ID, child.ID); err == nil {
		t.Fatal("resolving a node below a file must fail")
	}
}

// TestPathLockRecheckRejectsRenamedParent 复现“读到路径 → 暂停 → 路径被改名
// → 才拿到锁”的窗口，验证加锁后的复查会拒绝旧快照。
func TestPathLockRecheckRejectsRenamedParent(t *testing.T) {
	service, db, _, ownerA, _ := newTestService(t)
	ctx := context.Background()

	docs, err := service.CreateDirectory(ctx, ownerA.ID, "", "docs")
	if err != nil {
		t.Fatal(err)
	}
	before := countRows(t, db, &model.File{})

	type resolveResult struct {
		snapshot pathSnapshot
		err      error
	}
	resolved := make(chan resolveResult, 1)
	proceed := make(chan struct{})
	finished := make(chan error, 1)

	// 请求 A：先读到 /docs，然后暂停。
	go func() {
		snapshot, err := service.resolvePath(ctx, ownerA.ID, docs.ID)
		resolved <- resolveResult{snapshot: snapshot, err: err}
		if err != nil {
			finished <- err
			return
		}

		<-proceed

		release, err := service.acquirePathLocks(ctx, pathLockRequest{
			Snapshot: snapshot,
			Scope:    pathlock.Subtree,
		})
		if err != nil {
			finished <- err
			return
		}
		defer release()

		// 只有加锁和复查都成功，才允许执行真正的写入。
		_, err = service.CreateDirectory(ctx, ownerA.ID, docs.ID, "new")
		finished <- err
	}()

	first := <-resolved
	var renameErr error
	if first.err == nil {
		// 请求 B：拿锁、改名、释放锁。
		renameErr = func() error {
			snapshot, err := service.resolvePath(ctx, ownerA.ID, docs.ID)
			if err != nil {
				return err
			}
			release, err := service.acquirePathLocks(ctx, pathLockRequest{
				Snapshot: snapshot,
				Scope:    pathlock.Subtree,
			})
			if err != nil {
				return err
			}
			defer release()

			_, err = service.Rename(ctx, ownerA.ID, docs.ID, "archive")
			return err
		}()
	}

	// 请求 A 继续：申请旧路径 /docs 的锁一定成功，但复查会发现名字已变。
	close(proceed)
	runErr := <-finished

	if first.err != nil {
		t.Fatalf("request A resolve path: %v", first.err)
	}
	if renameErr != nil {
		t.Fatalf("request B rename: %v", renameErr)
	}
	if !errors.Is(runErr, ErrResourceChanged) {
		t.Fatalf("request A error = %v, want ErrResourceChanged", runErr)
	}

	// B 的改名已经提交。
	var stored model.File
	if err := db.WithContext(ctx).First(&stored, "id = ?", docs.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Name != "archive" {
		t.Fatalf("stored name = %q, want archive", stored.Name)
	}

	// A 没有按旧路径创建文件。
	after := countRows(t, db, &model.File{})
	if after != before {
		t.Fatalf("file rows changed from %d to %d after rejected operation", before, after)
	}
	var created int64
	if err := db.WithContext(ctx).Model(&model.File{}).
		Where("owner_id = ? AND parent_id = ? AND name = ?", ownerA.ID, docs.ID, "new").
		Count(&created).Error; err != nil {
		t.Fatal(err)
	}
	if created != 0 {
		t.Fatalf("request A created %d nodes despite failed recheck", created)
	}
}
