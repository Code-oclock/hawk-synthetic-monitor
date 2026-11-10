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

	"github.com/Code-oclock/hawk-synthetic-monitor/internal/hawk"
)

// fakeCollector имитирует collector Hawk: принимает POST и запоминает тело запроса.
func fakeCollector(t *testing.T, status int, body string, got *map[string]any) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("collector: метод %s, ожидался POST", r.Method)
		}
		if got != nil {
			_ = json.NewDecoder(r.Body).Decode(got)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
}

// fakeAPI имитирует GraphQL API. Запрос login выдаёт токен, запрос событий
// требует заголовок Authorization: Bearer tok-1 и отдаёт список titles.
// eventsAfter — сколько запросов событий вернуть пустой список, прежде чем показать события.
type fakeAPI struct {
	srv         *httptest.Server
	logins      atomic.Int32
	eventCalls  atomic.Int32
	titles      []string
	eventsAfter int32
	loginError  bool
}

func newFakeAPI(t *testing.T, titles ...string) *fakeAPI {
	t.Helper()
	f := &fakeAPI{titles: titles}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
				_, _ = w.Write([]byte(`{"errors":[{"message":"You must be signed in","extensions":{"code":"UNAUTHENTICATED"}}],"data":null}`))
				return
			}
			n := f.eventCalls.Add(1)
			evs := []string{}
			if n > f.eventsAfter {
				for i, title := range f.titles {
					evs = append(evs, `{"id":"id-`+string(rune('a'+i))+`","totalCount":1,"payload":{"title":"`+title+`"}}`)
				}
			}
			_, _ = w.Write([]byte(`{"data":{"project":{"events":[` + strings.Join(evs, ",") + `]}}}`))
		default:
			t.Errorf("api: неожиданный запрос %q", req.Query)
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
	var got map[string]any
	srv := fakeCollector(t, 200, `{"code":200,"error":false,"message":"OK"}`, &got)
	defer srv.Close()

	c := newClient(srv.URL, "http://unused")
	if err := c.SendEvent(context.Background(), "Synthetic check run-1"); err != nil {
		t.Fatalf("SendEvent: %v", err)
	}

	if got["token"] != "integration-token" {
		t.Errorf("token = %v, ожидался integration-token", got["token"])
	}
	if ct, _ := got["catcherType"].(string); ct == "" {
		t.Error("catcherType пустой: collector отвечает 400 CatcherType is empty")
	}
	payload, _ := got["payload"].(map[string]any)
	if payload["title"] != "Synthetic check run-1" {
		t.Errorf("payload.title = %v", payload["title"])
	}
}

func TestSendEvent_CollectorRejects(t *testing.T) {
	srv := fakeCollector(t, 400, `{"code":400,"error":true,"message":"Integration token invalid: abc"}`, nil)
	defer srv.Close()

	err := newClient(srv.URL, "http://unused").SendEvent(context.Background(), "x")

	var ce *hawk.CollectorError
	if !errors.As(err, &ce) {
		t.Fatalf("ожидался *CollectorError, получено %T: %v", err, err)
	}
	if ce.Code != 400 || !strings.Contains(ce.Message, "Integration token invalid") {
		t.Errorf("CollectorError = %+v", ce)
	}
}

func TestSendEvent_ContextCancelled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	err := newClient(srv.URL, "http://unused").SendEvent(ctx, "x")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ожидался context.DeadlineExceeded, получено: %v", err)
	}
}

func TestFindEvent_FindsByExactTitle(t *testing.T) {
	api := newFakeAPI(t, "other event", "Synthetic check run-2", "Synthetic check run-22")
	c := newClient("http://unused", api.srv.URL)

	ev, err := c.FindEvent(context.Background(), "Synthetic check run-2")
	if err != nil {
		t.Fatalf("FindEvent: %v", err)
	}
	if ev.ID != "id-b" || ev.Title != "Synthetic check run-2" {
		t.Errorf("найдено не то событие: %+v (run-22 — другое событие, сравнивать надо точно)", ev)
	}
}

func TestFindEvent_NotFound(t *testing.T) {
	api := newFakeAPI(t, "other event")
	c := newClient("http://unused", api.srv.URL)

	_, err := c.FindEvent(context.Background(), "missing")
	if !errors.Is(err, hawk.ErrNotFound) {
		t.Fatalf("ожидался ErrNotFound, получено: %v", err)
	}
}

func TestFindEvent_LoginOnce(t *testing.T) {
	api := newFakeAPI(t, "a")
	c := newClient("http://unused", api.srv.URL)

	for i := 0; i < 3; i++ {
		if _, err := c.FindEvent(context.Background(), "a"); err != nil {
			t.Fatalf("FindEvent #%d: %v", i, err)
		}
	}
	if n := api.logins.Load(); n != 1 {
		t.Errorf("login вызван %d раз, ожидался 1 (токен надо запомнить)", n)
	}
}

func TestFindEvent_LoginFails(t *testing.T) {
	api := newFakeAPI(t, "a")
	api.loginError = true
	c := newClient("http://unused", api.srv.URL)

	_, err := c.FindEvent(context.Background(), "a")
	if err == nil || !strings.Contains(err.Error(), "Wrong email or password") {
		t.Fatalf("ошибка GraphQL должна попасть в err вместе с текстом, получено: %v", err)
	}
	if errors.Is(err, hawk.ErrNotFound) {
		t.Error("ошибка входа не должна выглядеть как «событие не найдено»")
	}
}

func TestWaitForEvent_AppearsLater(t *testing.T) {
	api := newFakeAPI(t, "Synthetic check run-3")
	api.eventsAfter = 2 // первые два опроса пустые
	c := newClient("http://unused", api.srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ev, err := c.WaitForEvent(ctx, "Synthetic check run-3", 5*time.Millisecond)
	if err != nil {
		t.Fatalf("WaitForEvent: %v", err)
	}
	if ev.Title != "Synthetic check run-3" {
		t.Errorf("ev = %+v", ev)
	}
	if n := api.eventCalls.Load(); n != 3 {
		t.Errorf("запросов событий %d, ожидалось 3 (2 пустых + 1 успешный)", n)
	}
}

func TestWaitForEvent_Timeout(t *testing.T) {
	api := newFakeAPI(t) // событий нет вообще
	c := newClient("http://unused", api.srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := c.WaitForEvent(ctx, "never", 10*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ожидался context.DeadlineExceeded, получено: %v", err)
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Errorf("WaitForEvent завис на %v после таймаута", d)
	}
}

func TestWaitForEvent_LoginErrorStopsImmediately(t *testing.T) {
	api := newFakeAPI(t, "a")
	api.loginError = true
	c := newClient("http://unused", api.srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	_, err := c.WaitForEvent(ctx, "a", 10*time.Millisecond)
	if err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ошибка входа должна вернуться сразу, а не ждать таймаута: %v", err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Error("ошибку входа нельзя повторять до таймаута — это не «событие ещё не появилось»")
	}
}
