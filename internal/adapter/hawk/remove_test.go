package hawk_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Code-oclock/hawk-synthetic-monitor/internal/adapter/hawk"
	"github.com/Code-oclock/hawk-synthetic-monitor/internal/entity"
)

type recordedRequest struct {
	Method        string
	Authorization string
	ContentType   string
	Query         string
	Variables     map[string]any
}

func TestRemoveEvent_Success(t *testing.T) {
	var (
		gotReq recordedRequest
		logins atomic.Int32
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Query     string         `json:"query"`
			Variables map[string]any `json:"variables"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")

		switch {
		case strings.Contains(req.Query, "login"):
			logins.Add(1)
			_, _ = w.Write([]byte(`{"data":{"login":{"accessToken":"tok-1"}}}`))
		case strings.Contains(req.Query, "removeEvent"):
			gotReq = recordedRequest{
				Method:        r.Method,
				Authorization: r.Header.Get("Authorization"),
				ContentType:   r.Header.Get("Content-Type"),
				Query:         req.Query,
				Variables:     req.Variables,
			}
			_, _ = w.Write([]byte(`{"data":{"removeEvent":true}}`))
		default:
			t.Errorf("unexpected query: %s", req.Query)
		}
	}))
	defer srv.Close()

	c := newClient("http://unused", srv.URL)
	ev := entity.Event{
		ProjectID: "proj-custom",
		ID:        "ev-42",
	}

	if err := c.RemoveEvent(context.Background(), ev); err != nil {
		t.Fatalf("RemoveEvent: %v", err)
	}

	if logins.Load() != 1 {
		t.Errorf("logins = %d, want 1", logins.Load())
	}
	if gotReq.Method != http.MethodPost {
		t.Errorf("method = %s, want POST", gotReq.Method)
	}
	if gotReq.Authorization != "Bearer tok-1" {
		t.Errorf("auth = %s, want Bearer tok-1", gotReq.Authorization)
	}
	if !strings.Contains(gotReq.Query, "removeEvent(projectId: $projectId, eventId: $eventId)") {
		t.Errorf("query does not contain removeEvent mutation: %s", gotReq.Query)
	}
	if gotReq.Variables["projectId"] != "proj-custom" {
		t.Errorf("projectId = %v, want proj-custom", gotReq.Variables["projectId"])
	}
	if gotReq.Variables["eventId"] != "ev-42" {
		t.Errorf("eventId = %v, want ev-42", gotReq.Variables["eventId"])
	}
}

func TestRemoveEvent_ProjectIDFallback(t *testing.T) {
	var gotVars map[string]any

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Query     string         `json:"query"`
			Variables map[string]any `json:"variables"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")

		switch {
		case strings.Contains(req.Query, "login"):
			_, _ = w.Write([]byte(`{"data":{"login":{"accessToken":"tok-1"}}}`))
		case strings.Contains(req.Query, "removeEvent"):
			gotVars = req.Variables
			_, _ = w.Write([]byte(`{"data":{"removeEvent":true}}`))
		default:
			t.Errorf("unexpected query: %s", req.Query)
		}
	}))
	defer srv.Close()

	c := newClient("http://unused", srv.URL)
	ev := entity.Event{ID: "ev-99"}

	if err := c.RemoveEvent(context.Background(), ev); err != nil {
		t.Fatalf("RemoveEvent: %v", err)
	}

	if gotVars["projectId"] != "proj-1" {
		t.Errorf("projectId = %v, want fallback proj-1", gotVars["projectId"])
	}
	if gotVars["eventId"] != "ev-99" {
		t.Errorf("eventId = %v, want ev-99", gotVars["eventId"])
	}
}

func TestRemoveEvent_GraphQLError_NoErrProtocol(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Query string `json:"query"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")

		switch {
		case strings.Contains(req.Query, "login"):
			_, _ = w.Write([]byte(`{"data":{"login":{"accessToken":"tok-1"}}}`))
		case strings.Contains(req.Query, "removeEvent"):
			_, _ = w.Write([]byte(`{"errors":[{"message":"Not enough permissions"}],"data":null}`))
		default:
			t.Errorf("unexpected query: %s", req.Query)
		}
	}))
	defer srv.Close()

	c := newClient("http://unused", srv.URL)
	err := c.RemoveEvent(context.Background(), entity.Event{ID: "ev-1"})

	if err == nil {
		t.Fatal("want error on GraphQL error response")
	}
	if !strings.Contains(err.Error(), "Not enough permissions") {
		t.Errorf("err = %v, want to contain 'Not enough permissions'", err)
	}
	if !strings.Contains(err.Error(), "remove event:") {
		t.Errorf("err = %v, want to be wrapped with 'remove event:'", err)
	}
	if errors.Is(err, hawk.ErrProtocol) {
		t.Errorf("GraphQL error must not wrap ErrProtocol: %v", err)
	}
}

func TestRemoveEvent_False_ErrProtocol(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Query string `json:"query"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")

		switch {
		case strings.Contains(req.Query, "login"):
			_, _ = w.Write([]byte(`{"data":{"login":{"accessToken":"tok-1"}}}`))
		case strings.Contains(req.Query, "removeEvent"):
			_, _ = w.Write([]byte(`{"data":{"removeEvent":false}}`))
		default:
			t.Errorf("unexpected query: %s", req.Query)
		}
	}))
	defer srv.Close()

	c := newClient("http://unused", srv.URL)
	err := c.RemoveEvent(context.Background(), entity.Event{ID: "ev-1"})

	if !errors.Is(err, hawk.ErrProtocol) {
		t.Fatalf("want ErrProtocol on false response, got: %v", err)
	}
}

func TestRemoveEvent_MissingData_ErrProtocol(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Query string `json:"query"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")

		switch {
		case strings.Contains(req.Query, "login"):
			_, _ = w.Write([]byte(`{"data":{"login":{"accessToken":"tok-1"}}}`))
		case strings.Contains(req.Query, "removeEvent"):
			_, _ = w.Write([]byte(`{"data":null}`))
		default:
			t.Errorf("unexpected query: %s", req.Query)
		}
	}))
	defer srv.Close()

	c := newClient("http://unused", srv.URL)
	err := c.RemoveEvent(context.Background(), entity.Event{ID: "ev-1"})

	if !errors.Is(err, hawk.ErrProtocol) {
		t.Fatalf("want ErrProtocol on missing data, got: %v", err)
	}
}

func TestRemoveEvent_EmptyID_ZeroRequests(t *testing.T) {
	var totalRequests atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		totalRequests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := newClient("http://unused", srv.URL)
	err := c.RemoveEvent(context.Background(), entity.Event{ID: "", ProjectID: "proj-1"})

	if err == nil {
		t.Fatal("want error on empty event ID")
	}
	if n := totalRequests.Load(); n != 0 {
		t.Errorf("requests = %d, want 0", n)
	}
}

func TestRemoveEvent_UnauthenticatedRetryOnce(t *testing.T) {
	var (
		logins      atomic.Int32
		removeCalls atomic.Int32
		authHeaders []string
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Query string `json:"query"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")

		switch {
		case strings.Contains(req.Query, "login"):
			n := logins.Add(1)
			token := fmt.Sprintf("tok-%d", n)
			_, _ = fmt.Fprintf(w, `{"data":{"login":{"accessToken":%q}}}`, token)
		case strings.Contains(req.Query, "removeEvent"):
			authHeaders = append(authHeaders, r.Header.Get("Authorization"))
			n := removeCalls.Add(1)
			if n == 1 {
				_, _ = w.Write([]byte(`{"errors":[{"message":"You must be signed in","extensions":{"code":"UNAUTHENTICATED"}}],"data":null}`))

				return
			}
			_, _ = w.Write([]byte(`{"data":{"removeEvent":true}}`))
		default:
			t.Errorf("unexpected query: %s", req.Query)
		}
	}))
	defer srv.Close()

	c := newClient("http://unused", srv.URL)
	if err := c.RemoveEvent(context.Background(), entity.Event{ID: "ev-1"}); err != nil {
		t.Fatalf("RemoveEvent: %v", err)
	}

	if logins.Load() != 2 {
		t.Errorf("logins = %d, want 2", logins.Load())
	}
	if removeCalls.Load() != 2 {
		t.Errorf("removeCalls = %d, want 2", removeCalls.Load())
	}
	if len(authHeaders) != 2 {
		t.Fatalf("authHeaders count = %d, want 2", len(authHeaders))
	}
	if authHeaders[0] != "Bearer tok-1" {
		t.Errorf("first auth = %s, want Bearer tok-1", authHeaders[0])
	}
	if authHeaders[1] != "Bearer tok-2" {
		t.Errorf("second auth = %s, want Bearer tok-2", authHeaders[1])
	}
}
