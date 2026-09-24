// Package utils: thành phần dùng chung mọi layer (lỗi nghiệp vụ, logger, sinh id).
// Không phụ thuộc layer nào khác.
package utils

import (
	"errors"
	"fmt"
	"net/http"
)

// DomainError: lỗi nghiệp vụ / đầu vào; handler trả nguyên HTTPStatus + Code cho client.
type DomainError struct {
	HTTPStatus int
	Code       string
	Message    string
}

func (e *DomainError) Error() string { return e.Code + ": " + e.Message }

func NewDomainError(status int, code, format string, args ...any) *DomainError {
	return &DomainError{HTTPStatus: status, Code: code, Message: fmt.Sprintf(format, args...)}
}

// BadRequest: DomainError 400.
func BadRequest(code, format string, args ...any) *DomainError {
	return NewDomainError(http.StatusBadRequest, code, format, args...)
}

// AsDomainError: err (hoặc lỗi nó bọc) có phải DomainError không.
func AsDomainError(err error) (*DomainError, bool) {
	var e *DomainError
	ok := errors.As(err, &e)
	return e, ok
}
