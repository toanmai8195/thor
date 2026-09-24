package handler

import "thor/server/phonebook-service/internal/utils/phonedigest"

// Request / response JSON.

type checkRequest struct {
	V       int    `json:"v"`
	Root    string `json:"root"`
	Buckets string `json:"buckets,omitempty"`
}

type checkResponse struct {
	Status  string `json:"status"`
	Changed []int  `json:"changed,omitempty"`
}

type bucketBody struct {
	D        string              `json:"d,omitempty"`
	Contacts []phonedigest.Entry `json:"contacts"`
}

type uploadRequest struct {
	V       int                   `json:"v"`
	Buckets map[string]bucketBody `json:"buckets"`
}

type syncResponse struct {
	Root         string `json:"root,omitempty"`
	SyncID       string `json:"sync_id,omitempty"`
	Added        int    `json:"added"`
	Deleted      int    `json:"deleted"`
	ContactCount int    `json:"contact_cnt"`
	DeviceCount  int    `json:"device_contact_cnt"`
	Rejected     int    `json:"rejected"`
	Published    bool   `json:"published"`
}

type contactJSON struct {
	Phone string `json:"phone"`
	Name  string `json:"name"`
}

type listResponse struct {
	Contacts   []contactJSON `json:"contacts"`
	Total      int           `json:"total"`
	NextCursor *string       `json:"next_cursor"`
}

type deviceJSON struct {
	DeviceID     string `json:"device_id"`
	ContactCount int64  `json:"contact_cnt"`
	SyncedAt     int64  `json:"synced_at"`
}

type summaryResponse struct {
	UserID       int64        `json:"user_id"`
	ContactCount int64        `json:"contact_cnt"`
	Pending      bool         `json:"pending"`
	Devices      []deviceJSON `json:"devices"`
}
