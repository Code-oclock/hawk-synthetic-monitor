package rabbitmq_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Code-oclock/hawk-synthetic-monitor/internal/adapter/rabbitmq"
	"github.com/Code-oclock/hawk-synthetic-monitor/internal/entity"
	"github.com/Code-oclock/hawk-synthetic-monitor/internal/usecase"
)

func TestClient_ImplementsQueueInspector(t *testing.T) {
	t.Parallel()

	var _ usecase.QueueInspector = (*rabbitmq.Client)(nil)
}

func TestQueues_FilteringAndOrdering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cfgQueues  []string
		serverBody string
		want       []entity.QueueStat
	}{
		{
			name:      "happy path with order and filtering",
			cfgQueues: []string{"queue-b", "queue-a"},
			serverBody: `[
				{"name": "queue-c", "messages": 15, "consumers": 2},
				{"name": "queue-a", "messages": 5, "consumers": 1},
				{"name": "queue-b", "messages": 20, "consumers": 3}
			]`,
			want: []entity.QueueStat{
				{Name: "queue-b", Messages: 20, Consumers: 3, Missing: false},
				{Name: "queue-a", Messages: 5, Consumers: 1, Missing: false},
			},
		},
		{
			name:      "missing queue",
			cfgQueues: []string{"queue-present", "queue-missing"},
			serverBody: `[
				{"name": "queue-present", "messages": 8, "consumers": 2}
			]`,
			want: []entity.QueueStat{
				{Name: "queue-present", Messages: 8, Consumers: 2, Missing: false},
				{Name: "queue-missing", Messages: 0, Consumers: 0, Missing: true},
			},
		},
		{
			name:      "empty Queues returns all sorted",
			cfgQueues: nil,
			serverBody: `[
				{"name": "zebra", "messages": 1, "consumers": 1},
				{"name": "alpha", "messages": 10, "consumers": 2},
				{"name": "middle", "messages": 5, "consumers": 3}
			]`,
			want: []entity.QueueStat{
				{Name: "alpha", Messages: 10, Consumers: 2, Missing: false},
				{Name: "middle", Messages: 5, Consumers: 3, Missing: false},
				{Name: "zebra", Messages: 1, Consumers: 1, Missing: false},
			},
		},
		{
			name:       "empty Queues and empty response",
			cfgQueues:  []string{},
			serverBody: `[]`,
			want:       []entity.QueueStat{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Errorf("method = %s, want GET", r.Method)
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(tt.serverBody))
			}))
			defer srv.Close()

			client := rabbitmq.New(rabbitmq.Config{
				BaseURL: srv.URL,
				Queues:  tt.cfgQueues,
			}, srv.Client())

			got, err := client.Queues(context.Background())
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got stats:\n%+v\nwant:\n%+v", got, tt.want)
			}
		})
	}
}

func TestQueues_VhostEscaping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		vhost     string
		wantPath  string
		wantQuery string
	}{
		{
			name:      "default slash vhost",
			vhost:     "/",
			wantPath:  "/api/queues/%2F",
			wantQuery: "name,messages,consumers",
		},
		{
			name:      "empty vhost defaults to slash",
			vhost:     "",
			wantPath:  "/api/queues/%2F",
			wantQuery: "name,messages,consumers",
		},
		{
			name:      "vhost with subpaths",
			vhost:     "project/alpha",
			wantPath:  "/api/queues/project%2Falpha",
			wantQuery: "name,messages,consumers",
		},
		{
			name:      "standard name vhost",
			vhost:     "production",
			wantPath:  "/api/queues/production",
			wantQuery: "name,messages,consumers",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var (
				gotEscapedPath string
				gotColumns     string
				gotMethod      string
			)

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotMethod = r.Method
				gotEscapedPath = r.URL.EscapedPath()
				gotColumns = r.URL.Query().Get("columns")

				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte("[]"))
			}))
			defer srv.Close()

			client := rabbitmq.New(rabbitmq.Config{
				BaseURL: srv.URL,
				Vhost:   tt.vhost,
			}, srv.Client())

			_, err := client.Queues(context.Background())
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if gotMethod != http.MethodGet {
				t.Errorf("method = %q, want GET", gotMethod)
			}
			if gotEscapedPath != tt.wantPath {
				t.Errorf("escaped path = %q, want %q", gotEscapedPath, tt.wantPath)
			}
			if gotColumns != tt.wantQuery {
				t.Errorf("columns query = %q, want %q", gotColumns, tt.wantQuery)
			}
		})
	}
}

func TestQueues_BasicAuth(t *testing.T) {
	t.Parallel()

	var (
		gotUser string
		gotPass string
		gotOK   bool
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUser, gotPass, gotOK = r.BasicAuth()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[]"))
	}))
	defer srv.Close()

	client := rabbitmq.New(rabbitmq.Config{
		BaseURL:  srv.URL,
		User:     "monitor_user",
		Password: "super_secret_password",
	}, srv.Client())

	_, err := client.Queues(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !gotOK {
		t.Fatal("basic auth not sent")
	}
	if gotUser != "monitor_user" || gotPass != "super_secret_password" {
		t.Errorf("basic auth credentials mismatch: got (%q, %q)", gotUser, gotPass)
	}
}

func TestQueues_Status401(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"not_authorized","reason":"Access refused: monitor"}`))
	}))
	defer srv.Close()

	client := rabbitmq.New(rabbitmq.Config{
		BaseURL:  srv.URL,
		User:     "monitor",
		Password: "wrong_password",
	}, srv.Client())

	_, err := client.Queues(context.Background())
	if err == nil {
		t.Fatal("expected error on 401, got nil")
	}

	if !strings.Contains(err.Error(), "401") {
		t.Errorf("error %q does not contain '401'", err.Error())
	}
	if strings.Contains(err.Error(), "wrong_password") {
		t.Errorf("error %q leaks password", err.Error())
	}
}

func TestQueues_Non2xxTruncation(t *testing.T) {
	t.Parallel()

	largeBody := strings.Repeat("error detail line\n", 200) // ~3800 bytes
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(largeBody))
	}))
	defer srv.Close()

	client := rabbitmq.New(rabbitmq.Config{
		BaseURL: srv.URL,
	}, srv.Client())

	_, err := client.Queues(context.Background())
	if err == nil {
		t.Fatal("expected error on 500, got nil")
	}

	if !strings.Contains(err.Error(), "500") {
		t.Errorf("error %q does not contain '500'", err.Error())
	}
	if len(err.Error()) > 1200 {
		t.Errorf("error message length %d exceeds expected limit", len(err.Error()))
	}
}

func TestQueues_InvalidJSON(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{not valid json`))
	}))
	defer srv.Close()

	client := rabbitmq.New(rabbitmq.Config{
		BaseURL: srv.URL,
	}, srv.Client())

	_, err := client.Queues(context.Background())
	if err == nil {
		t.Fatal("expected error on invalid json, got nil")
	}
}

func TestQueues_ContextCanceled(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(300 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[]"))
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	client := rabbitmq.New(rabbitmq.Config{
		BaseURL: srv.URL,
	}, srv.Client())

	_, err := client.Queues(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}
}

func TestQueues_NeverPutPasswordInError(t *testing.T) {
	t.Parallel()

	secret := "ultra_confidential_pass_123"

	// 1. Password in server 401 response body.
	srv401 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte("refused password ultra_confidential_pass_123"))
	}))
	defer srv401.Close()

	client401 := rabbitmq.New(rabbitmq.Config{
		BaseURL:  srv401.URL,
		Password: secret,
	}, srv401.Client())

	_, err := client401.Queues(context.Background())
	if err == nil {
		t.Fatal("expected error for 401, got nil")
	}
	if strings.Contains(err.Error(), secret) {
		t.Errorf("error %q leaks password", err.Error())
	}

	// 2. Network error on closed server.
	srvClosed := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srvClosedURL := srvClosed.URL
	srvClosed.Close()

	clientClosed := rabbitmq.New(rabbitmq.Config{
		BaseURL:  srvClosedURL,
		Password: secret,
	}, &http.Client{Timeout: 50 * time.Millisecond})

	_, err = clientClosed.Queues(context.Background())
	if err == nil {
		t.Fatal("expected network error, got nil")
	}
	if strings.Contains(err.Error(), secret) {
		t.Errorf("network error %q leaks password", err.Error())
	}
}

func TestNew_NilClient(t *testing.T) {
	t.Parallel()

	client := rabbitmq.New(rabbitmq.Config{
		BaseURL: "http://localhost:15672",
	}, nil)

	if client == nil {
		t.Fatal("expected non-nil Client")
	}
}
