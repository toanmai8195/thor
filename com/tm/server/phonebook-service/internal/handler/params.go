package handler

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"

	"thor/server/phonebook-service/internal/controller"
	"thor/server/phonebook-service/internal/utils"
)

const (
	// maxBodyBytes: body sau khi giải nén (5.000 contact ≈ 250 KB JSON)
	maxBodyBytes = 4 << 20
	defaultLimit = 500
	maxLimit     = 2000
	maxUserID    = 1<<53 - 1
)

var (
	b64          = base64.RawURLEncoding
	deviceIDExpr = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
)

func toSyncResponse(res controller.SyncResult) syncResponse {
	out := syncResponse{SyncID: res.SyncID, Added: res.Added, Deleted: res.Deleted, ContactCount: res.ContactCount,
		DeviceCount: res.DeviceCount, Rejected: res.Rejected, Published: res.Published}
	if res.Root != nil {
		out.Root = b64.EncodeToString(res.Root)
	}
	return out
}

func parseUserID(w http.ResponseWriter, r *http.Request, log *slog.Logger) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("userId"), 10, 64)
	if err != nil || id < 1 || id > maxUserID {
		writeError(w, log, badRequest("INVALID_USER_ID", "userId phải là số nguyên 1..2^53-1"))
		return 0, false
	}
	return id, true
}

func parseIDs(w http.ResponseWriter, r *http.Request, log *slog.Logger) (int64, string, bool) {
	userID, ok := parseUserID(w, r, log)
	if !ok {
		return 0, "", false
	}
	deviceID := r.PathValue("deviceId")
	if !deviceIDExpr.MatchString(deviceID) {
		writeError(w, log, badRequest("INVALID_DEVICE_ID", "deviceId phải khớp %s", deviceIDExpr))
		return 0, "", false
	}
	return userID, deviceID, true
}

// decode đọc JSON body (giải nén nếu gzip), giới hạn kích thước, không nhận field lạ.
func decodeBody(w http.ResponseWriter, r *http.Request, v any, log *slog.Logger) bool {
	var body io.Reader = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	if r.Header.Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(body)
		if err != nil {
			writeError(w, log, badRequest("INVALID_BODY", "gzip không hợp lệ"))
			return false
		}
		defer gz.Close()
		body = io.LimitReader(gz, maxBodyBytes+1)
	}
	data, err := io.ReadAll(body)
	if err != nil {
		writeError(w, log, utils.NewDomainError(http.StatusRequestEntityTooLarge, "BODY_TOO_LARGE", "body tối đa %d byte", maxBodyBytes))
		return false
	}
	if len(data) > maxBodyBytes {
		writeError(w, log, utils.NewDomainError(http.StatusRequestEntityTooLarge, "BODY_TOO_LARGE", "body sau giải nén tối đa %d byte", maxBodyBytes))
		return false
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeError(w, log, badRequest("INVALID_BODY", "JSON không hợp lệ: %v", err))
		return false
	}
	return true
}
