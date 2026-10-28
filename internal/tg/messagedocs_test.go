package tg

import (
	"context"
	"strings"
	"testing"

	"github.com/gotd/td/tg"
)

// A channels.getMessages answer: a message with a document, one without
// media, an empty document, a service message, a deleted id (MessageEmpty)
// and an id the answer leaves out; a message nobody asked for is ignored.
func TestMessageDocsOf(t *testing.T) {
	msgs := []tg.MessageClass{
		&tg.Message{ID: 10, Media: &tg.MessageMediaDocument{Document: &tg.Document{ID: 1, Size: 4096}}},
		&tg.Message{ID: 11, Message: "text only"},
		&tg.Message{ID: 12, Media: &tg.MessageMediaDocument{Document: &tg.DocumentEmpty{ID: 2}}},
		&tg.MessageService{ID: 13},
		&tg.MessageEmpty{ID: 14},
		&tg.Message{ID: 99, Media: &tg.MessageMediaDocument{Document: &tg.Document{ID: 3, Size: 7}}},
	}
	got := messageDocsOf([]int{10, 11, 12, 13, 14, 15}, msgs)
	want := map[int]MessageDoc{10: {HasDoc: true, Size: 4096}, 11: {}, 12: {}, 13: {}}
	if len(got) != len(want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	for id, w := range want {
		if g, ok := got[id]; !ok || g != w {
			t.Errorf("id %d: %+v (present %v), want %+v", id, g, ok, w)
		}
	}
}

func TestMessageDocsBatchLimit(t *testing.T) {
	s := &Session{}
	ids := make([]int, MaxMessagesPerGet+1)
	if _, err := s.MessageDocs(context.Background(), ids); err == nil || !strings.Contains(err.Error(), "at most") {
		t.Fatalf("101 ids: %v", err)
	}
	if got, err := s.MessageDocs(context.Background(), nil); err != nil || len(got) != 0 {
		t.Fatalf("no ids: %v %v", got, err)
	}
}
