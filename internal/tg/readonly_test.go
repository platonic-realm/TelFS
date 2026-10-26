package tg

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// A read-only session refuses every channel write before touching the
// network (no API is even connected here).
func TestReadOnlySessionRefusesWrites(t *testing.T) {
	s := &Session{}
	s.SetReadOnly(true)
	ctx := context.Background()
	if _, err := s.UploadDocument(ctx, strings.NewReader("x"), "blob", "c"); !errors.Is(err, ErrReadOnly) {
		t.Errorf("UploadDocument: %v", err)
	}
	if _, err := s.PostMessage(ctx, "x"); !errors.Is(err, ErrReadOnly) {
		t.Errorf("PostMessage: %v", err)
	}
	if err := s.DeleteChannelMessages(ctx, 1); !errors.Is(err, ErrReadOnly) {
		t.Errorf("DeleteChannelMessages: %v", err)
	}
	if err := s.PinMessage(ctx, 1); !errors.Is(err, ErrReadOnly) {
		t.Errorf("PinMessage: %v", err)
	}
	if err := s.Unpin(ctx, 1); !errors.Is(err, ErrReadOnly) {
		t.Errorf("Unpin: %v", err)
	}
	if _, err := s.UploadSnapshot(ctx, []byte("x"), 1, 1, "fs"); !errors.Is(err, ErrReadOnly) {
		t.Errorf("UploadSnapshot: %v", err)
	}
}
