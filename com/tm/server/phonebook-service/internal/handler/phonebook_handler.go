// Package handler: parse / kiểm tra tham số HTTP, gọi Controller, ghi response, đổi lỗi → HTTP status.
// Không chứa luật nghiệp vụ, không đụng HBase / Kafka.
//
//	POST   /v1/users/{userId}/devices/{deviceId}/phonebook/check      Check
//	PUT    /v1/users/{userId}/devices/{deviceId}/phonebook/buckets    Upload
//	DELETE /v1/users/{userId}/devices/{deviceId}/phonebook            DeleteDevice
//	GET    /v1/users/{userId}/phonebook/contacts?limit=&cursor=&device_id=   List
//	GET    /v1/users/{userId}/phonebook/contacts/{phone}              Lookup
//	GET    /v1/users/{userId}/phonebook/summary                       Summary
//
// Digest / cursor gửi dạng base64url không padding. Body có thể gzip (Content-Encoding: gzip).
package handler

import (
	"encoding/binary"
	"log/slog"
	"net/http"
	"strconv"

	"thor/server/phonebook-service/internal/controller"
	"thor/server/phonebook-service/internal/utils/phonedigest"
)

// PhonebookHandler: các endpoint danh bạ.
type PhonebookHandler struct {
	ctrl *controller.PhonebookController
	log  *slog.Logger
}

func NewPhonebookHandler(ctrl *controller.PhonebookController, log *slog.Logger) *PhonebookHandler {
	return &PhonebookHandler{ctrl: ctrl, log: log}
}

func (h *PhonebookHandler) Check(w http.ResponseWriter, r *http.Request) {
	userID, deviceID, ok := parseIDs(w, r, h.log)
	if !ok {
		return
	}
	var body checkRequest
	if !decodeBody(w, r, &body, h.log) {
		return
	}
	root, err := b64.DecodeString(body.Root)
	if err != nil {
		writeError(w, h.log, badRequest("INVALID_ROOT", "root phải là base64url"))
		return
	}
	var buckets []byte
	if body.Buckets != "" {
		if buckets, err = b64.DecodeString(body.Buckets); err != nil {
			writeError(w, h.log, badRequest("INVALID_BUCKETS", "buckets phải là base64url"))
			return
		}
	}
	res, err := h.ctrl.Check(r.Context(), userID, deviceID, controller.CheckRequest{V: body.V, Root: root, Buckets: buckets})
	if err != nil {
		writeError(w, h.log, err)
		return
	}
	writeJSON(w, http.StatusOK, checkResponse{Status: res.Status, Changed: res.Changed})
}

func (h *PhonebookHandler) Upload(w http.ResponseWriter, r *http.Request) {
	userID, deviceID, ok := parseIDs(w, r, h.log)
	if !ok {
		return
	}
	var body uploadRequest
	if !decodeBody(w, r, &body, h.log) {
		return
	}
	req := controller.UploadRequest{V: body.V, Buckets: make(map[int]controller.BucketUpload, len(body.Buckets))}
	for key, b := range body.Buckets {
		k, err := strconv.Atoi(key)
		if err != nil {
			writeError(w, h.log, badRequest("INVALID_BUCKET", "bucket %q phải là số 00..99", key))
			return
		}
		if _, dup := req.Buckets[k]; dup {
			writeError(w, h.log, badRequest("INVALID_BUCKET", "bucket %d bị gửi 2 lần", k))
			return
		}
		var d []byte
		if b.D != "" {
			if d, err = b64.DecodeString(b.D); err != nil || len(d) != phonedigest.TruncLen {
				writeError(w, h.log, badRequest("INVALID_DIGEST", "d của bucket %s phải là base64url 4 byte", key))
				return
			}
		}
		req.Buckets[k] = controller.BucketUpload{Digest: d, Contacts: b.Contacts}
	}
	res, err := h.ctrl.Upload(r.Context(), userID, deviceID, req)
	if err != nil {
		writeError(w, h.log, err)
		return
	}
	writeJSON(w, http.StatusOK, toSyncResponse(res))
}

func (h *PhonebookHandler) DeleteDevice(w http.ResponseWriter, r *http.Request) {
	userID, deviceID, ok := parseIDs(w, r, h.log)
	if !ok {
		return
	}
	res, err := h.ctrl.DeleteDevice(r.Context(), userID, deviceID)
	if err != nil {
		writeError(w, h.log, err)
		return
	}
	writeJSON(w, http.StatusOK, toSyncResponse(res))
}

func (h *PhonebookHandler) List(w http.ResponseWriter, r *http.Request) {
	userID, ok := parseUserID(w, r, h.log)
	if !ok {
		return
	}
	q := r.URL.Query()
	limit := defaultLimit
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > maxLimit {
			writeError(w, h.log, badRequest("INVALID_LIMIT", "limit phải trong 1..%d", maxLimit))
			return
		}
		limit = n
	}
	var cursor uint64
	if v := q.Get("cursor"); v != "" {
		raw, err := b64.DecodeString(v)
		if err != nil || len(raw) != 8 {
			writeError(w, h.log, badRequest("INVALID_CURSOR", "cursor không hợp lệ"))
			return
		}
		cursor = binary.BigEndian.Uint64(raw)
	}
	deviceID := q.Get("device_id")
	if deviceID != "" && !deviceIDExpr.MatchString(deviceID) {
		writeError(w, h.log, badRequest("INVALID_DEVICE_ID", "device_id phải khớp %s", deviceIDExpr))
		return
	}
	res, err := h.ctrl.List(r.Context(), userID, deviceID, limit, cursor)
	if err != nil {
		writeError(w, h.log, err)
		return
	}
	out := listResponse{Total: res.Total, Contacts: make([]contactJSON, 0, len(res.Contacts))}
	for _, c := range res.Contacts {
		out.Contacts = append(out.Contacts, contactJSON{Phone: c.Phone, Name: c.Name})
	}
	if res.NextCursor != 0 {
		next := b64.EncodeToString(binary.BigEndian.AppendUint64(nil, res.NextCursor))
		out.NextCursor = &next
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *PhonebookHandler) Lookup(w http.ResponseWriter, r *http.Request) {
	userID, ok := parseUserID(w, r, h.log)
	if !ok {
		return
	}
	c, err := h.ctrl.Lookup(r.Context(), userID, r.PathValue("phone"))
	if err != nil {
		writeError(w, h.log, err)
		return
	}
	writeJSON(w, http.StatusOK, contactJSON{Phone: c.Phone, Name: c.Name})
}

func (h *PhonebookHandler) Summary(w http.ResponseWriter, r *http.Request) {
	userID, ok := parseUserID(w, r, h.log)
	if !ok {
		return
	}
	s, err := h.ctrl.Summary(r.Context(), userID)
	if err != nil {
		writeError(w, h.log, err)
		return
	}
	out := summaryResponse{UserID: userID, ContactCount: s.ContactCount, Pending: s.Pending, Devices: []deviceJSON{}}
	for _, d := range s.Devices {
		out.Devices = append(out.Devices, deviceJSON{DeviceID: d.DeviceID, ContactCount: d.ContactCount, SyncedAt: d.SyncedAt})
	}
	writeJSON(w, http.StatusOK, out)
}
