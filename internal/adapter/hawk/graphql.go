package hawk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/Code-oclock/hawk-synthetic-monitor/internal/usecase"
)

const (
	codeUnauthenticated = "UNAUTHENTICATED"
	codeTokenExpired    = "ACCESS_TOKEN_EXPIRED_ERROR"
)

type graphQLError struct {
	message string
	code    string
}

func (e *graphQLError) Error() string {
	return "graphql: " + e.message
}

func isUnauthenticated(err error) bool {
	var gqlErr *graphQLError

	return errors.As(err, &gqlErr) && (gqlErr.code == codeUnauthenticated || gqlErr.code == codeTokenExpired)
}

// query runs a GraphQL query.
func (c *Client) query(ctx context.Context, query string, vars map[string]any, bearer string, out any) error {
	body := map[string]any{"query": query, "variables": vars}

	status, respBody, err := c.postJSON(ctx, c.cfg.APIURL, body, bearer)
	if err != nil {
		return err
	}

	if status == http.StatusTooManyRequests || (status >= 500 && status <= 599) {
		return fmt.Errorf("%w: graphql status %d", usecase.ErrTransient, status)
	}

	var resp struct {
		Data   json.RawMessage `json:"data"`
		Errors []struct {
			Message    string `json:"message"`
			Extensions struct {
				Code string `json:"code"`
			} `json:"extensions"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return fmt.Errorf("%w: graphql response not json: %w", ErrProtocol, err)
	}

	if len(resp.Errors) > 0 {
		first := resp.Errors[0]

		return &graphQLError{message: first.Message, code: first.Extensions.Code}
	}
	if status != http.StatusOK {
		return fmt.Errorf("graphql: unexpected status %d", status)
	}

	if out != nil && len(resp.Data) > 0 {
		if err := json.Unmarshal(resp.Data, out); err != nil {
			return fmt.Errorf("%w: decode graphql data: %w", ErrProtocol, err)
		}
	}

	return nil
}

// accessToken logs in on first use and reuses the token afterwards.
func (c *Client) accessToken(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.token != "" {
		return c.token, nil
	}

	var out struct {
		Login *struct {
			AccessToken string `json:"accessToken"`
		} `json:"login"`
	}
	const mutation = `mutation($email: String!, $password: String!) {
		login(email: $email, password: $password) { accessToken }
	}`
	vars := map[string]any{"email": c.cfg.Email, "password": c.cfg.Password}
	if err := c.query(ctx, mutation, vars, "", &out); err != nil {
		return "", fmt.Errorf("login: %w", err)
	}

	if out.Login == nil || out.Login.AccessToken == "" {
		return "", fmt.Errorf("%w: missing access token in login response", ErrProtocol)
	}

	c.token = out.Login.AccessToken

	return c.token, nil
}

// dropToken forgets a rejected token unless another goroutine has already replaced it.
func (c *Client) dropToken(rejected string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.token == rejected {
		c.token = ""
	}
}
