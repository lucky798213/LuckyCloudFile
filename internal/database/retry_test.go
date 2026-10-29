package database

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
)

func TestIsRetryableConflict(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "deadlock", err: &mysqlDriver.MySQLError{Number: 1213}, want: true},
		{name: "lock wait timeout", err: &mysqlDriver.MySQLError{Number: 1205}, want: true},
		{
			name: "wrapped deadlock",
			err:  fmt.Errorf("publish content: %w", &mysqlDriver.MySQLError{Number: 1213}),
			want: true,
		},
		{name: "duplicate key", err: &mysqlDriver.MySQLError{Number: 1062}, want: false},
		{name: "plain error", err: errors.New("deadlock"), want: false},
		{name: "nil", err: nil, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := IsRetryableConflict(test.err); got != test.want {
				t.Fatalf("IsRetryableConflict(%v) = %v, want %v", test.err, got, test.want)
			}
		})
	}
}

func TestWaitForRetryHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	if err := WaitForRetry(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitForRetry = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("cancelled wait took %v", elapsed)
	}
}
