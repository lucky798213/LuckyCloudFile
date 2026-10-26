package storage

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestRegistryGet(t *testing.T) {
	registry := NewRegistry()
	store := NewLocal(t.TempDir())
	if err := registry.Register(7, store); err != nil {
		t.Fatal(err)
	}

	got, err := registry.Get(7)
	if err != nil {
		t.Fatal(err)
	}
	if got != store {
		t.Fatal("registry returned a different store")
	}

	// 写入后能通过同一个 Store 读回，确认 Registry 没有复制或包装错对象。
	if _, err := got.Save(context.Background(), "key", strings.NewReader("hello"), 1<<20); err != nil {
		t.Fatal(err)
	}
	if _, err := got.Open(context.Background(), "key"); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryGetMissingPolicy(t *testing.T) {
	registry := NewRegistry()
	if _, err := registry.Get(42); !errors.Is(err, ErrPolicyNotFound) {
		t.Fatalf("wanted ErrPolicyNotFound, got %v", err)
	}
}

func TestRegistryRejectsDuplicateAndInvalidPolicy(t *testing.T) {
	var registry Registry
	store := NewLocal(t.TempDir())

	if err := registry.Register(0, store); err == nil {
		t.Fatal("policy id 0 must be rejected")
	}
	if err := registry.Register(1, nil); err == nil {
		t.Fatal("nil store must be rejected")
	}
	if err := registry.Register(1, store); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(1, NewLocal(t.TempDir())); err == nil {
		t.Fatal("duplicate policy id must be rejected")
	}
	if _, err := registry.Get(1); err != nil {
		t.Fatalf("zero-value registry did not keep the registered store: %v", err)
	}
}
