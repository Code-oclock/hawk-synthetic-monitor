package hawk_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Code-oclock/hawk-synthetic-monitor/internal/adapter/hawk"
	"github.com/Code-oclock/hawk-synthetic-monitor/internal/entity"
	"github.com/Code-oclock/hawk-synthetic-monitor/internal/usecase"
)

func fakeCollector(t *testing.T, status int, contentType, body string, got map[string]any) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("collector: method %s, want POST", r.Method)
		}
		if got != nil {
			_ = json.NewDecoder(r.Body).Decode(&got)
		}
		if contentType != "" {
			w.Header().Set("Content-Type", contentType)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
}

type fakeAPI struct {
	srv         *httptest.Server
	logins      atomic.Int32
	eventCalls  atomic.Int32
	titles      []string
	eventsAfter int32
	loginError  bool
	projectNull bool
	emptyID     bool
	statusCode  int
	rawBody     string
}

func newFakeAPI(t *testing.T, titles ...string) *fakeAPI {
	t.Helper()

	f := &fakeAPI{titles: titles}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if f.statusCode != 0 {
			w.WriteHeader(f.statusCode)
			if f.rawBody != "" {
				_, _ = w.Write([]byte(f.rawBody))
			}

			return
		}

		var req struct {
			Query string `json:"query"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")

		switch {
		case strings.Contains(req.Query, "login"):
			f.logins.Add(1)
			if f.loginError {
				_, _ = w.Write([]byte(`{"errors":[{"message":"Wrong email or password"}],"data":null}`))

				return
			}
			_, _ = w.Write([]byte(`{"data":{"login":{"accessToken":"tok-1"}}}`))
		case strings.Contains(req.Query, "events"):
			if r.Header.Get("Authorization") != "Bearer tok-1" {
				_, _ = w.Write([]byte(`{"errors":[{"message":"You must be signed in"}],"data":null}`))

				return
			}

			if f.projectNull {
				_, _ = w.Write([]byte(`{"data":{"project":null}}`))

				return
			}

			evs := []string{}
			if f.eventCalls.Add(1) > f.eventsAfter {
				for i, title := range f.titles {
					id := "id-" + string(rune('a'+i))
					if f.emptyID {
						id = ""
					}
					evs = append(evs, `{"id":"`+id+`","totalCount":1,"catcherType":"errors/golang","payload":{"title":"`+title+`","type":"error"}}`)
				}
			}
			_, _ = w.Write([]byte(`{"data":{"project":{"events":[` + strings.Join(evs, ",") + `]}}}`))
		default:
			t.Errorf("api: unexpected query %q", req.Query)
		}
	}))
	t.Cleanup(f.srv.Close)

	return f
}

func newClient(collectorURL, apiURL string) *hawk.Client {
	return hawk.New(hawk.Config{
		CollectorURL:     collectorURL,
		APIURL:           apiURL,
		IntegrationToken: "integration-token",
		ProjectID:        "proj-1",
		Email:            "monitor@example.com",
		Password:         "secret",
	})
}

func TestSendEvent_OK(t *testing.T) {
	got := map[string]any{}
	srv := fakeCollector(t, 200, "application/json", `{"code":200,"error":false,"message":"OK"}`, got)
	defer srv.Close()

	if err := newClient(srv.URL, "http://unused").SendEvent(context.Background(), "Synthetic check run-1"); err != nil {
		t.Fatalf("SendEvent: %v", err)
	}

	if got["token"] != "integration-token" {
		t.Errorf("token = %v, want integration-token", got["token"])
	}
	if ct, _ := got["catcherType"].(string); ct == "" {
		t.Error("catcherType is empty: the collector answers 400 CatcherType is empty")
	}
	payload, _ := got["payload"].(map[string]any)
	if payload["title"] != "Synthetic check run-1" {
		t.Errorf("payload.title = %v", payload["title"])
	}
}

func TestSendEvent_CollectorRejects(t *testing.T) {
	srv := fakeCollector(t, 400, "application/json", `{"code":400,"error":true,"message":"Integration token invalid: abc"}`, nil)
	defer srv.Close()

	err := newClient(srv.URL, "http://unused").SendEvent(context.Background(), "x")

	var ce *hawk.CollectorError
	if !errors.As(err, &ce) {
		t.Fatalf("want *CollectorError, got %T: %v", err, err)
	}
	if ce.Code != 400 || !strings.Contains(ce.Message, "Integration token invalid") {
		t.Errorf("CollectorError = %+v", ce)
	}
	if errors.Is(err, usecase.ErrTransient) {
		t.Error("400 CollectorError must not be transient")
	}
}

func TestSendEvent_200HTML_ErrProtocol(t *testing.T) {
	srv := fakeCollector(t, 200, "text/html", "<html>Error</html>", nil)
	defer srv.Close()

	err := newClient(srv.URL, "http://unused").SendEvent(context.Background(), "x")
	if !errors.Is(err, hawk.ErrProtocol) {
		t.Fatalf("want ErrProtocol on 200 HTML, got: %v", err)
	}
	if errors.Is(err, usecase.ErrTransient) {
		t.Error("protocol error must not be transient")
	}
}

func TestSendEvent_200MissingError_ErrProtocol(t *testing.T) {
	srv := fakeCollector(t, 200, "application/json", `{"code":200,"message":"missing error field"}`, nil)
	defer srv.Close()

	err := newClient(srv.URL, "http://unused").SendEvent(context.Background(), "x")
	if !errors.Is(err, hawk.ErrProtocol) {
		t.Fatalf("want ErrProtocol on 200 without error, got: %v", err)
	}
}

func TestSendEvent_200Null_ErrProtocol(t *testing.T) {
	srv := fakeCollector(t, 200, "application/json", "null", nil)
	defer srv.Close()

	err := newClient(srv.URL, "http://unused").SendEvent(context.Background(), "x")
	if !errors.Is(err, hawk.ErrProtocol) {
		t.Fatalf("want ErrProtocol on 200 null, got: %v", err)
	}
}

func TestSendEvent_200ErrorTrue_CollectorError(t *testing.T) {
	srv := fakeCollector(t, 200, "application/json", `{"code":200,"error":true,"message":"custom rejection"}`, nil)
	defer srv.Close()

	err := newClient(srv.URL, "http://unused").SendEvent(context.Background(), "x")

	var ce *hawk.CollectorError
	if !errors.As(err, &ce) {
		t.Fatalf("want *CollectorError on 200 error:true, got: %v", err)
	}
	if ce.Code != 200 || ce.Message != "custom rejection" {
		t.Errorf("CollectorError = %+v", ce)
	}
	if errors.Is(err, usecase.ErrTransient) {
		t.Error("200 error:true must not be transient")
	}
}

func TestSendEvent_502HTML_TransientAndCollectorError(t *testing.T) {
	srv := fakeCollector(t, 502, "text/html", "<html>Bad Gateway</html>", nil)
	defer srv.Close()

	err := newClient(srv.URL, "http://unused").SendEvent(context.Background(), "x")
	if !errors.Is(err, usecase.ErrTransient) {
		t.Fatalf("want ErrTransient on 502 HTML, got: %v", err)
	}

	var ce *hawk.CollectorError
	if !errors.As(err, &ce) {
		t.Fatalf("want *CollectorError on 502 HTML, got: %v", err)
	}
	if ce.Code != 502 {
		t.Errorf("ce.Code = %d, want 502", ce.Code)
	}
}

func TestSendEvent_429_TransientAndCollectorError(t *testing.T) {
	srv := fakeCollector(t, 429, "application/json", `{"code":429,"error":true,"message":"rate limit"}`, nil)
	defer srv.Close()

	err := newClient(srv.URL, "http://unused").SendEvent(context.Background(), "x")
	if !errors.Is(err, usecase.ErrTransient) {
		t.Fatalf("want ErrTransient on 429, got: %v", err)
	}

	var ce *hawk.CollectorError
	if !errors.As(err, &ce) {
		t.Fatalf("want *CollectorError on 429, got: %v", err)
	}
	if ce.Code != 429 || ce.Message != "rate limit" {
		t.Errorf("CollectorError = %+v", ce)
	}
}

func TestSendEvent_ClosedServer_Transient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srv.Close()

	err := newClient(srv.URL, "http://unused").SendEvent(context.Background(), "x")
	if !errors.Is(err, usecase.ErrTransient) {
		t.Fatalf("want ErrTransient on closed server, got: %v", err)
	}
}

func TestSendEvent_ContextCancelled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		time.Sleep(300 * time.Millisecond)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := newClient(srv.URL, "http://unused").SendEvent(ctx, "x")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got: %v", err)
	}
	if errors.Is(err, usecase.ErrTransient) {
		t.Error("canceled ctx must not be transient")
	}
}

func TestHealth_OK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			t.Errorf("path = %s, want /health", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"redis_status":true,"mongo_db_status":true}`))
	}))
	defer srv.Close()

	c := hawk.New(hawk.Config{CollectorURL: srv.URL})
	if err := c.Health(context.Background()); err != nil {
		t.Fatalf("Health: %v", err)
	}

	cCustom := hawk.New(hawk.Config{HealthURL: srv.URL + "/health"})
	if err := cCustom.Health(context.Background()); err != nil {
		t.Fatalf("Health with custom URL: %v", err)
	}
}

func TestHealth_RedisStatusFalse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"redis_status":false,"mongo_db_status":true}`))
	}))
	defer srv.Close()

	c := hawk.New(hawk.Config{CollectorURL: srv.URL})
	err := c.Health(context.Background())
	if err == nil {
		t.Fatal("want error when redis_status is false")
	}
	if errors.Is(err, usecase.ErrTransient) {
		t.Error("unhealthy status must not be transient")
	}
}

func TestHealth_500(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal error"))
	}))
	defer srv.Close()

	c := hawk.New(hawk.Config{CollectorURL: srv.URL})
	err := c.Health(context.Background())
	if !errors.Is(err, usecase.ErrTransient) {
		t.Fatalf("want ErrTransient on 500, got: %v", err)
	}
}

func TestHealth_NonJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html>ok</html>"))
	}))
	defer srv.Close()

	c := hawk.New(hawk.Config{CollectorURL: srv.URL})
	err := c.Health(context.Background())
	if err == nil {
		t.Fatal("want error on non-JSON body")
	}
	if errors.Is(err, usecase.ErrTransient) {
		t.Error("non-JSON 200 must not be transient")
	}
}

func TestFindEvent_FindsByExactTitle(t *testing.T) {
	api := newFakeAPI(t, "other event", "Synthetic check run-2", "Synthetic check run-22")

	ev, err := newClient("http://unused", api.srv.URL).FindEvent(context.Background(), "Synthetic check run-2")
	if err != nil {
		t.Fatalf("FindEvent: %v", err)
	}
	if ev.ID != "id-b" || ev.Title != "Synthetic check run-2" {
		t.Errorf("wrong event: %+v (run-22 is a different event, titles must match exactly)", ev)
	}
}

func TestFindEvent_NotFound(t *testing.T) {
	api := newFakeAPI(t, "other event")

	_, err := newClient("http://unused", api.srv.URL).FindEvent(context.Background(), "missing")
	if !errors.Is(err, usecase.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got: %v", err)
	}
}

func TestFindEvent_ProjectNull(t *testing.T) {
	api := newFakeAPI(t)
	api.projectNull = true

	_, err := newClient("http://unused", api.srv.URL).FindEvent(context.Background(), "a")
	if !errors.Is(err, hawk.ErrProtocol) {
		t.Fatalf("want ErrProtocol on project:null, got: %v", err)
	}
	if errors.Is(err, usecase.ErrNotFound) {
		t.Error("project:null must NOT be ErrNotFound")
	}
}

func TestFindEvent_EmptyID(t *testing.T) {
	api := newFakeAPI(t, "a")
	api.emptyID = true

	_, err := newClient("http://unused", api.srv.URL).FindEvent(context.Background(), "a")
	if !errors.Is(err, hawk.ErrProtocol) {
		t.Fatalf("want ErrProtocol on empty ID, got: %v", err)
	}
}

func TestFindEvent_FieldsMapped(t *testing.T) {
	api := newFakeAPI(t, "Title-1")

	ev, err := newClient("http://unused", api.srv.URL).FindEvent(context.Background(), "Title-1")
	if err != nil {
		t.Fatalf("FindEvent: %v", err)
	}
	want := entity.Event{
		ProjectID:   "proj-1",
		ID:          "id-a",
		Title:       "Title-1",
		CatcherType: "errors/golang",
		Type:        "error",
		TotalCount:  1,
	}
	if ev != want {
		t.Errorf("ev = %+v, want %+v", ev, want)
	}
}

func TestFindEvent_GraphQLNonJSON(t *testing.T) {
	api := newFakeAPI(t)
	api.rawBody = "<html>not json</html>"
	api.statusCode = http.StatusOK

	_, err := newClient("http://unused", api.srv.URL).FindEvent(context.Background(), "a")
	if !errors.Is(err, hawk.ErrProtocol) {
		t.Fatalf("want ErrProtocol on non-JSON GraphQL body, got: %v", err)
	}
}

func TestFindEvent_GraphQL500(t *testing.T) {
	api := newFakeAPI(t)
	api.statusCode = http.StatusInternalServerError

	_, err := newClient("http://unused", api.srv.URL).FindEvent(context.Background(), "a")
	if !errors.Is(err, usecase.ErrTransient) {
		t.Fatalf("want ErrTransient on GraphQL 500, got: %v", err)
	}
}

func TestFindEvent_GraphQL429(t *testing.T) {
	api := newFakeAPI(t)
	api.statusCode = http.StatusTooManyRequests

	_, err := newClient("http://unused", api.srv.URL).FindEvent(context.Background(), "a")
	if !errors.Is(err, usecase.ErrTransient) {
		t.Fatalf("want ErrTransient on GraphQL 429, got: %v", err)
	}
}

func TestFindEvent_LoginOnce(t *testing.T) {
	api := newFakeAPI(t, "a")
	c := newClient("http://unused", api.srv.URL)

	for i := range 3 {
		if _, err := c.FindEvent(context.Background(), "a"); err != nil {
			t.Fatalf("FindEvent #%d: %v", i, err)
		}
	}
	if n := api.logins.Load(); n != 1 {
		t.Errorf("login called %d times, want 1 (the token must be reused)", n)
	}
}

func TestFindEvent_LoginFails(t *testing.T) {
	api := newFakeAPI(t, "a")
	api.loginError = true

	_, err := newClient("http://unused", api.srv.URL).FindEvent(context.Background(), "a")
	if err == nil || !strings.Contains(err.Error(), "Wrong email or password") {
		t.Fatalf("the GraphQL error text must be part of err, got: %v", err)
	}
	if errors.Is(err, usecase.ErrNotFound) {
		t.Error("a login failure must not look like a missing event")
	}
}
