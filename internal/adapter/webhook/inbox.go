// Package webhook provides an HTTP delivery inbox for Hawk notifications.
package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/Code-oclock/hawk-synthetic-monitor/internal/entity"
	"github.com/Code-oclock/hawk-synthetic-monitor/internal/usecase"
)

const maxBodyBytes = 1 << 20

// ErrAlreadySubscribed indicates the delivery key is already registered.
var ErrAlreadySubscribed = errors.New("already subscribed")

var (
	_ usecase.DeliveryInbox  = (*Inbox)(nil)
	_ usecase.DeliveryWaiter = (*waiter)(nil)
)

// Inbox registers delivery waiters and handles incoming webhooks.
type Inbox struct {
	now     func() time.Time
	mu      sync.Mutex
	entries map[entity.DeliveryKey]*entry
}

type entry struct {
	ch chan entity.Delivery
}

type waiter struct {
	inbox *Inbox
	key   entity.DeliveryKey
	entry *entry
	once  sync.Once
}

type notificationRequest struct {
	Type    string       `json:"type"`
	Payload *payloadData `json:"payload"`
}

type payloadData struct {
	Project *projectData `json:"project"`
	Events  []eventItem  `json:"events"`
}

type projectData struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type eventItem struct {
	Event    eventDetails `json:"event"`
	NewCount int          `json:"newCount"`
}

type eventDetails struct {
	ID          string `json:"id"`
	Title       string `json:"title"`
	TotalCount  int    `json:"totalCount"`
	CatcherType string `json:"catcherType"`
}

// NewInbox creates a new webhook delivery inbox.
func NewInbox(now func() time.Time) *Inbox {
	if now == nil {
		now = time.Now
	}

	return &Inbox{
		now:     now,
		entries: make(map[entity.DeliveryKey]*entry),
	}
}

// Subscribe registers a waiter for the given delivery key.
func (i *Inbox) Subscribe(key entity.DeliveryKey) (usecase.DeliveryWaiter, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	if _, exists := i.entries[key]; exists {
		return nil, ErrAlreadySubscribed
	}

	ent := &entry{
		ch: make(chan entity.Delivery, 1),
	}
	i.entries[key] = ent

	return &waiter{
		inbox: i,
		key:   key,
		entry: ent,
	}, nil
}

// Handler returns an HTTP handler for incoming Hawk notifications.
func (i *Inbox) Handler() http.Handler {
	return http.HandlerFunc(i.serveHTTP)
}

func (i *Inbox) publish(d entity.Delivery) {
	i.mu.Lock()
	defer i.mu.Unlock()

	ent, ok := i.entries[d.Key]
	if !ok {
		return
	}

	select {
	case ent.ch <- d:
	default:
	}
}

func (i *Inbox) unsubscribe(key entity.DeliveryKey, ent *entry) {
	i.mu.Lock()
	defer i.mu.Unlock()

	if current, ok := i.entries[key]; ok && current == ent {
		delete(i.entries, key)
	}
}

// Wait blocks until a delivery arrives or the context is done.
func (w *waiter) Wait(ctx context.Context) (entity.Delivery, error) {
	select {
	case d := <-w.entry.ch:
		return d, nil
	default:
	}

	select {
	case d := <-w.entry.ch:
		return d, nil
	case <-ctx.Done():
		return entity.Delivery{}, ctx.Err()
	}
}

// Close unsubscribes the waiter from the inbox.
func (w *waiter) Close() {
	w.once.Do(func() {
		w.inbox.unsubscribe(w.key, w.entry)
	})
}

func (i *Inbox) serveHTTP(w http.ResponseWriter, r *http.Request) {
	receivedAt := i.now()

	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if r.Header.Get("X-Hawk-Notification") == "" {
		http.Error(w, "missing X-Hawk-Notification header", http.StatusBadRequest)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			http.Error(w, "request entity too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "bad request", http.StatusBadRequest)

		return
	}

	var req notificationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	if req.Type != "event" && req.Type != "several-events" {
		w.WriteHeader(http.StatusOK)
		return
	}

	if req.Payload == nil || req.Payload.Project == nil || req.Payload.Project.ID == "" || len(req.Payload.Events) == 0 {
		w.WriteHeader(http.StatusOK)
		return
	}

	for _, item := range req.Payload.Events {
		key := entity.DeliveryKey{
			ProjectID: req.Payload.Project.ID,
			Title:     item.Event.Title,
		}
		i.publish(entity.Delivery{
			Key:        key,
			EventID:    item.Event.ID,
			ReceivedAt: receivedAt,
		})
	}

	w.WriteHeader(http.StatusOK)
}
