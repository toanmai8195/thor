// Package client gọi REST API của friend-service.
package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"thor/com/tm/friend-simulator/internal/sim"
)

type Client struct {
	base string
	http *http.Client
}

func New(baseURL string, timeout time.Duration) *Client {
	return &Client{base: baseURL, http: &http.Client{Timeout: timeout}}
}

// endpoint: method + path cho mỗi hành động (README 10.6)
func endpoint(s sim.Step) (string, string) {
	a, t := s.Actor, s.Target
	switch s.Action {
	case sim.ActRequest:
		return http.MethodPost, fmt.Sprintf("/v1/users/%d/requests/%d", a, t)
	case sim.ActCancel:
		return http.MethodDelete, fmt.Sprintf("/v1/users/%d/requests/%d", a, t)
	case sim.ActAccept:
		return http.MethodPost, fmt.Sprintf("/v1/users/%d/requests/%d/accept", a, t)
	case sim.ActReject:
		return http.MethodPost, fmt.Sprintf("/v1/users/%d/requests/%d/reject", a, t)
	case sim.ActUnfriend:
		return http.MethodDelete, fmt.Sprintf("/v1/users/%d/friends/%d", a, t)
	case sim.ActBlock:
		return http.MethodPost, fmt.Sprintf("/v1/users/%d/blocks/%d", a, t)
	}
	panic("hành động không hỗ trợ: " + string(s.Action))
}

// Do thực hiện 1 hành động, trả HTTP status.
func (c *Client) Do(ctx context.Context, s sim.Step) (int, error) {
	method, path := endpoint(s)
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, nil)
	if err != nil {
		return 0, err
	}
	res, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	_, _ = io.Copy(io.Discard, res.Body)
	return res.StatusCode, nil
}

// Outgoing trả status của actor→target theo friend-service ("" nếu chưa có quan hệ).
func (c *Client) Outgoing(ctx context.Context, actor, target int64) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/v1/users/%d/relationships/%d", c.base, actor, target), nil)
	if err != nil {
		return "", err
	}
	res, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return "", fmt.Errorf("relationships trả %d", res.StatusCode)
	}
	var body struct {
		Outgoing *struct {
			Status string `json:"status"`
		} `json:"outgoing"`
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return "", err
	}
	if body.Outgoing == nil {
		return "", nil
	}
	return body.Outgoing.Status, nil
}
