package file

import (
	"crypto/sha256"
	"testing"
)

// TestChunkLedgerTracksUniqueReceipts 验证账本只累计一次编号，
// 重复登记不会改变字节数，查找和删除也能正常工作。
func TestChunkLedgerTracksUniqueReceipts(t *testing.T) {
	ledger := newChunkLedger()
	const sessionID = "session-1"

	ledger.create(sessionID)
	if received, ok := ledger.received(sessionID); !ok || received != 0 {
		t.Fatalf("new ledger received=(%d, %v), want (0, true)", received, ok)
	}

	receipt := ChunkReceipt{
		Size:   5,
		SHA256: sha256.Sum256([]byte("hello")),
	}
	if received, recorded := ledger.record(sessionID, 0, receipt); !recorded || received != 5 {
		t.Fatalf("record first chunk = (%d, %v), want (5, true)", received, recorded)
	}
	if received, recorded := ledger.record(sessionID, 0, receipt); recorded || received != 5 {
		t.Fatalf("record duplicate chunk = (%d, %v), want (5, false)", received, recorded)
	}

	another := ChunkReceipt{
		Size:   3,
		SHA256: sha256.Sum256([]byte("bye")),
	}
	if received, recorded := ledger.record(sessionID, 1, another); !recorded || received != 8 {
		t.Fatalf("record second chunk = (%d, %v), want (8, true)", received, recorded)
	}

	if got, ok := ledger.lookup(sessionID, 0); !ok || got != receipt {
		t.Fatalf("lookup chunk 0 = (%+v, %v), want %+v", got, ok, receipt)
	}
	if _, ok := ledger.lookup(sessionID, 2); ok {
		t.Fatal("lookup missing chunk succeeded")
	}

	// create 重复调用不能清空进度。
	ledger.create(sessionID)
	if received, _ := ledger.received(sessionID); received != 8 {
		t.Fatalf("re-create cleared progress: received=%d", received)
	}

	ledger.remove(sessionID)
	if _, ok := ledger.received(sessionID); ok {
		t.Fatal("removed session is still in ledger")
	}
	if _, recorded := ledger.record("missing", 0, receipt); recorded {
		t.Fatal("recording into a missing ledger succeeded")
	}
}
