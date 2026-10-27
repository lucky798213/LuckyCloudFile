package file

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

// TestSnapshotAndProtectSources 验证来源快照记录身份，压缩前重新取得保护
// 并读到同一份内容；Release 可以重复调用。
func TestSnapshotAndProtectSources(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	source, err := env.service.Upload(
		ctx, env.ownerA.ID, "", "a.txt", strings.NewReader("hello"),
	)
	if err != nil {
		t.Fatal(err)
	}

	snapshots, err := env.service.SnapshotSources(
		ctx, env.ownerA.ID, []string{source.ID},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 1 ||
		snapshots[0].FileID != source.ID ||
		snapshots[0].Name != "a.txt" ||
		snapshots[0].Size != 5 ||
		snapshots[0].EntityID == 0 {
		t.Fatalf("unexpected snapshot: %+v", snapshots)
	}

	guard, err := env.service.ProtectSources(ctx, env.ownerA.ID, snapshots)
	if err != nil {
		t.Fatal(err)
	}
	current, reader, err := guard.Open(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	guard.Release()
	guard.Release()

	if current.FileID != source.ID || string(data) != "hello" {
		t.Fatalf("read source = %+v %q", current, data)
	}

	// 别人的文件不能记录成来源。
	if _, err := env.service.SnapshotSources(
		ctx, env.ownerB.ID, []string{source.ID},
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("other owner snapshot error = %v, want ErrNotFound", err)
	}
}

// TestProtectSourcesRejectsChangedSource 验证快照之后改名/换内容会明确失败。
func TestProtectSourcesRejectsChangedSource(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	source, err := env.service.Upload(
		ctx, env.ownerA.ID, "", "a.txt", strings.NewReader("hello"),
	)
	if err != nil {
		t.Fatal(err)
	}
	snapshots, err := env.service.SnapshotSources(
		ctx, env.ownerA.ID, []string{source.ID},
	)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := env.service.Rename(ctx, env.ownerA.ID, source.ID, "renamed.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := env.service.ProtectSources(
		ctx, env.ownerA.ID, snapshots,
	); !errors.Is(err, ErrSourceChanged) {
		t.Fatalf("renamed source error = %v, want ErrSourceChanged", err)
	}

	// 换一份内容（新 Entity）同样必须失败。
	replacement, err := env.service.Upload(
		ctx, env.ownerA.ID, "", "b.txt", strings.NewReader("other"),
	)
	if err != nil {
		t.Fatal(err)
	}
	replacementSnapshots, err := env.service.SnapshotSources(
		ctx, env.ownerA.ID, []string{replacement.ID},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := env.service.Delete(ctx, env.ownerA.ID, replacement.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := env.service.ProtectSources(
		ctx, env.ownerA.ID, replacementSnapshots,
	); !errors.Is(err, ErrSourceChanged) {
		t.Fatalf("deleted source error = %v, want ErrSourceChanged", err)
	}
}

// TestProtectSourcesRejectsMovedSource 验证来源被移动到另一个目录后
// 即使名字、内容和大小都没变，快照核对仍然失败。
func TestProtectSourcesRejectsMovedSource(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	source, err := env.service.Upload(
		ctx, env.ownerA.ID, "", "a.txt", strings.NewReader("hello"),
	)
	if err != nil {
		t.Fatal(err)
	}
	snapshots, err := env.service.SnapshotSources(
		ctx, env.ownerA.ID, []string{source.ID},
	)
	if err != nil {
		t.Fatal(err)
	}

	directory, err := env.service.CreateDirectory(ctx, env.ownerA.ID, "", "docs")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.service.Move(ctx, env.ownerA.ID, source.ID, directory.ID); err != nil {
		t.Fatal(err)
	}

	if _, err := env.service.ProtectSources(
		ctx, env.ownerA.ID, snapshots,
	); !errors.Is(err, ErrSourceChanged) {
		t.Fatalf("moved source error = %v, want ErrSourceChanged", err)
	}
}
