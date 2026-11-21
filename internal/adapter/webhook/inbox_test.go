package webhook_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Code-oclock/hawk-synthetic-monitor/internal/adapter/webhook"
	"github.com/Code-oclock/hawk-synthetic-monitor/internal/entity"
)

func makeHawkBody(notificationType, projectID, title, eventID string) string {
	return fmt.Sprintf(`{
		"type": %q,
		"payload": {
			"project": {"id": %q, "name": "Test Project"},
			"events": [
				{
					"event": {
						"id": %q,
						"title": %q,
						"totalCount": 1,
						"catcherType": "errors/golang"
					},
					"newCount": 1
				}
			],
			"period": null
		}
	}`, notificationType, projectID, eventID, title)
}

func postNotification(handler http.Handler, body, headerVal string) *httptest.ResponseRecorder {
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/webhook", strings.NewReader(body))
	if headerVal != "" {
		req.Header.Set("X-Hawk-Notification", headerVal)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	return rec
}

func TestInbox_SubscribePostWait(t *testing.T) {
	fixedTime := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	inbox := webhook.NewInbox(func() time.Time { return fixedTime })
	key := entity.DeliveryKey{ProjectID: "proj-1", Title: "title-1"}

	waiter, err := inbox.Subscribe(key)
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}
	defer waiter.Close()

	body := makeHawkBody("event", "proj-1", "title-1", "event-123")
	rec := postNotification(inbox.Handler(), body, "event")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	d, err := waiter.Wait(ctx)
	if err != nil {
		t.Fatalf("unexpected wait error: %v", err)
	}
	if d.Key != key {
		t.Fatalf("expected key %+v, got %+v", key, d.Key)
	}
	if d.EventID != "event-123" {
		t.Fatalf("expected EventID event-123, got %q", d.EventID)
	}
	if !d.ReceivedAt.Equal(fixedTime) {
		t.Fatalf("expected ReceivedAt %v, got %v", fixedTime, d.ReceivedAt)
	}
}

func TestInbox_EarlyDelivery(t *testing.T) {
	fixedTime := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	inbox := webhook.NewInbox(func() time.Time { return fixedTime })
	key := entity.DeliveryKey{ProjectID: "proj-1", Title: "title-1"}

	waiter, err := inbox.Subscribe(key)
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}
	defer waiter.Close()

	body := makeHawkBody("event", "proj-1", "title-1", "event-early")
	rec := postNotification(inbox.Handler(), body, "event")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	d, err := waiter.Wait(ctx)
	if err != nil {
		t.Fatalf("unexpected wait error: %v", err)
	}
	if d.EventID != "event-early" {
		t.Fatalf("expected EventID event-early, got %q", d.EventID)
	}
	if !d.ReceivedAt.Equal(fixedTime) {
		t.Fatalf("expected ReceivedAt %v, got %v", fixedTime, d.ReceivedAt)
	}
}

func TestInbox_DuplicateDeliveryFirstWins(t *testing.T) {
	inbox := webhook.NewInbox(nil)
	key := entity.DeliveryKey{ProjectID: "proj-1", Title: "title-1"}

	waiter, err := inbox.Subscribe(key)
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}
	defer waiter.Close()

	body1 := makeHawkBody("event", "proj-1", "title-1", "event-first")
	rec1 := postNotification(inbox.Handler(), body1, "event")
	if rec1.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec1.Code)
	}

	body2 := makeHawkBody("event", "proj-1", "title-1", "event-second")
	rec2 := postNotification(inbox.Handler(), body2, "event")
	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec2.Code)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	d, err := waiter.Wait(ctx)
	if err != nil {
		t.Fatalf("unexpected wait error: %v", err)
	}
	if d.EventID != "event-first" {
		t.Fatalf("expected first delivery event-first, got %q", d.EventID)
	}
}

func TestInbox_UnknownKeyIgnored(t *testing.T) {
	inbox := webhook.NewInbox(nil)
	body := makeHawkBody("event", "unknown-proj", "unknown-title", "event-1")
	rec := postNotification(inbox.Handler(), body, "event")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}
}

func TestInbox_ForeignProjectSameTitle(t *testing.T) {
	inbox := webhook.NewInbox(nil)
	key := entity.DeliveryKey{ProjectID: "proj-1", Title: "shared-title"}

	waiter, err := inbox.Subscribe(key)
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}
	defer waiter.Close()

	body := makeHawkBody("event", "foreign-proj", "shared-title", "event-foreign")
	rec := postNotification(inbox.Handler(), body, "event")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err = waiter.Wait(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

func TestInbox_WaitTimeout(t *testing.T) {
	inbox := webhook.NewInbox(nil)
	key := entity.DeliveryKey{ProjectID: "proj-1", Title: "title-1"}

	waiter, err := inbox.Subscribe(key)
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}
	defer waiter.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err = waiter.Wait(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

func TestInbox_CloseThenLateDelivery(t *testing.T) {
	inbox := webhook.NewInbox(nil)
	key := entity.DeliveryKey{ProjectID: "proj-1", Title: "title-1"}

	waiter, err := inbox.Subscribe(key)
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}

	waiter.Close()
	waiter.Close()

	body := makeHawkBody("event", "proj-1", "title-1", "event-late")
	rec := postNotification(inbox.Handler(), body, "event")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}
}

func TestInbox_DuplicateSubscribeError(t *testing.T) {
	inbox := webhook.NewInbox(nil)
	key := entity.DeliveryKey{ProjectID: "proj-1", Title: "title-1"}

	waiter1, err := inbox.Subscribe(key)
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}
	defer waiter1.Close()

	waiter2, err := inbox.Subscribe(key)
	if err == nil {
		t.Fatal("expected error on duplicate subscribe, got nil")
	}
	if waiter2 != nil {
		t.Fatalf("expected nil waiter on error, got %+v", waiter2)
	}

	waiter1.Close()
	waiter3, err := inbox.Subscribe(key)
	if err != nil {
		t.Fatalf("expected successful subscribe after close, got %v", err)
	}
	waiter3.Close()
}

func TestInbox_TwoIndependentKeysConcurrently(t *testing.T) {
	inbox := webhook.NewInbox(nil)
	key1 := entity.DeliveryKey{ProjectID: "p1", Title: "t1"}
	key2 := entity.DeliveryKey{ProjectID: "p2", Title: "t2"}

	waiter1, err := inbox.Subscribe(key1)
	if err != nil {
		t.Fatalf("subscribe key1: %v", err)
	}
	defer waiter1.Close()

	waiter2, err := inbox.Subscribe(key2)
	if err != nil {
		t.Fatalf("subscribe key2: %v", err)
	}
	defer waiter2.Close()

	var wg sync.WaitGroup
	wg.Add(4)

	go func() {
		defer wg.Done()
		body := makeHawkBody("event", "p1", "t1", "ev-1")
		rec := postNotification(inbox.Handler(), body, "event")
		if rec.Code != http.StatusOK {
			t.Errorf("post key1: expected 200, got %d", rec.Code)
		}
	}()

	go func() {
		defer wg.Done()
		body := makeHawkBody("event", "p2", "t2", "ev-2")
		rec := postNotification(inbox.Handler(), body, "event")
		if rec.Code != http.StatusOK {
			t.Errorf("post key2: expected 200, got %d", rec.Code)
		}
	}()

	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		d1, err := waiter1.Wait(ctx)
		if err != nil {
			t.Errorf("wait key1: %v", err)
			return
		}
		if d1.EventID != "ev-1" {
			t.Errorf("key1 EventID: expected ev-1, got %q", d1.EventID)
		}
	}()

	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		d2, err := waiter2.Wait(ctx)
		if err != nil {
			t.Errorf("wait key2: %v", err)
			return
		}
		if d2.EventID != "ev-2" {
			t.Errorf("key2 EventID: expected ev-2, got %q", d2.EventID)
		}
	}()

	wg.Wait()
}

func TestInbox_MethodNotAllowed(t *testing.T) {
	inbox := webhook.NewInbox(nil)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/webhook", http.NoBody)
	rec := httptest.NewRecorder()
	inbox.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 Method Not Allowed, got %d", rec.Code)
	}
}

func TestInbox_InvalidJSON(t *testing.T) {
	inbox := webhook.NewInbox(nil)
	rec := postNotification(inbox.Handler(), "{invalid json", "event")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request, got %d", rec.Code)
	}
}

func TestInbox_MissingHeader(t *testing.T) {
	inbox := webhook.NewInbox(nil)
	body := makeHawkBody("event", "p1", "t1", "ev-1")
	rec := postNotification(inbox.Handler(), body, "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request, got %d", rec.Code)
	}
}

func TestInbox_BodyOver1MiB(t *testing.T) {
	inbox := webhook.NewInbox(nil)
	largeBody := bytes.Repeat([]byte("a"), (1<<20)+1024)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/webhook", bytes.NewReader(largeBody))
	req.Header.Set("X-Hawk-Notification", "event")
	rec := httptest.NewRecorder()
	inbox.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest && rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 400 or 413, got %d", rec.Code)
	}
}

func TestInbox_SeveralEventsType(t *testing.T) {
	inbox := webhook.NewInbox(nil)
	key := entity.DeliveryKey{ProjectID: "proj-1", Title: "title-several"}

	waiter, err := inbox.Subscribe(key)
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}
	defer waiter.Close()

	body := makeHawkBody("several-events", "proj-1", "title-several", "event-sev")
	rec := postNotification(inbox.Handler(), body, "several-events")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	d, err := waiter.Wait(ctx)
	if err != nil {
		t.Fatalf("unexpected wait error: %v", err)
	}
	if d.EventID != "event-sev" {
		t.Fatalf("expected event-sev, got %q", d.EventID)
	}
}

func TestInbox_IgnoredNotificationTypes(t *testing.T) {
	inbox := webhook.NewInbox(nil)
	key := entity.DeliveryKey{ProjectID: "proj-1", Title: "title-1"}

	waiter, err := inbox.Subscribe(key)
	if err != nil {
		t.Fatalf("unexpected subscribe error: %v", err)
	}
	defer waiter.Close()

	body := makeHawkBody("issue", "proj-1", "title-1", "ev-issue")
	rec := postNotification(inbox.Handler(), body, "issue")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err = waiter.Wait(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

func TestInbox_MissingProjectOrEvents(t *testing.T) {
	inbox := webhook.NewInbox(nil)

	cases := []string{
		`{"type":"event","payload":null}`,
		`{"type":"event","payload":{"project":null}}`,
		`{"type":"event","payload":{"project":{"id":""},"events":[]}}`,
		`{"type":"event","payload":{"project":{"id":"p1"},"events":[]}}`,
		`{"type":"event","payload":{"project":{"id":"p1"},"events":null}}`,
	}

	for i, payload := range cases {
		rec := postNotification(inbox.Handler(), payload, "event")
		if rec.Code != http.StatusOK {
			t.Fatalf("case %d: expected 200 OK, got %d", i, rec.Code)
		}
	}
}

func TestInbox_ConcurrentDeliverAndClose(t *testing.T) {
	inbox := webhook.NewInbox(nil)
	key := entity.DeliveryKey{ProjectID: "p1", Title: "t1"}

	for i := 0; i < 50; i++ {
		waiter, err := inbox.Subscribe(key)
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			body := makeHawkBody("event", "p1", "t1", "ev")
			postNotification(inbox.Handler(), body, "event")
		}()

		go func() {
			defer wg.Done()
			waiter.Close()
		}()

		wg.Wait()
		waiter.Close()
	}
}
