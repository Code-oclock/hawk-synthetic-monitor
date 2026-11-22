package hawk_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Code-oclock/hawk-synthetic-monitor/internal/adapter/hawk"
)

func TestFindEvent_LogsInAgainWhenTokenExpired(t *testing.T) {
	rejections := map[string]string{
		"unauthenticated": `{"errors":[{"message":"jwt expired","extensions":{"code":"UNAUTHENTICATED"}}],"data":null}`,
		"expired":         `{"errors":[{"message":"access expired, log in again","extensions":{"code":"ACCESS_TOKEN_EXPIRED_ERROR"}}],"data":null}`,
	}
	for name, rejection := range rejections {
		t.Run(name, func(t *testing.T) {
			testLogsInAgain(t, rejection)
		})
	}
}

func testLogsInAgain(t *testing.T, rejection string) {
	t.Helper()

	var logins atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Query string `json:"query"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		if strings.Contains(req.Query, "login") {
			n := logins.Add(1)
			_, _ = w.Write([]byte(`{"data":{"login":{"accessToken":"tok-` + strconv.Itoa(int(n)) + `"}}}`))

			return
		}

		// the first token is expired, the second one is accepted
		if r.Header.Get("Authorization") != "Bearer tok-2" {
			_, _ = w.Write([]byte(rejection))

			return
		}
		_, _ = w.Write([]byte(`{"data":{"project":{"events":[{"id":"e1","totalCount":1,"catcherType":"errors/golang","payload":{"title":"a","type":"error"}}]}}}`))
	}))
	defer srv.Close()

	c := hawk.New(hawk.Config{APIURL: srv.URL, ProjectID: "p", Email: "e", Password: "p"})

	ev, err := c.FindEvent(context.Background(), "a")
	if err != nil {
		t.Fatalf("FindEvent: %v", err)
	}
	if ev.ID != "e1" {
		t.Errorf("ev = %+v", ev)
	}
	if n := logins.Load(); n != 2 {
		t.Errorf("logins = %d, want 2 (initial and after expiry)", n)
	}
}
