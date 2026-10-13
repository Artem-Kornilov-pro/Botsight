package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

const maxBodyBytes = 64 << 10

// Error codes are stable; the dashboard maps them to messages.
const (
	codeInvalidJSON        = "invalid_json"
	codeValidation         = "validation_failed"
	codeUnsupportedMedia   = "unsupported_media_type"
	codeUnauthorized       = "unauthorized"
	codeCSRF               = "csrf_failed"
	codeForbidden          = "forbidden"
	codeNotFound           = "not_found"
	codeMethodNotAllowed   = "method_not_allowed"
	codeRateLimited        = "rate_limited"
	codeInternal           = "internal"
	codeUnavailable        = "unavailable"
	codeEmailTaken         = "email_taken"
	codeInvalidCredentials = "invalid_credentials"
	codeWrongPassword      = "wrong_password"
	codeNoPassword         = "no_password"
	codeNoEmail            = "no_email"
	codeAlreadyVerified    = "already_verified"
	codeInvalidToken       = "invalid_token"
	codeTelegramDisabled   = "telegram_disabled"
	codeTelegramInvalid    = "invalid_telegram_auth"
	codeTelegramTaken      = "telegram_taken"
	codeAccountNotFound    = "account_not_found"
	codeAlreadyMember      = "already_member"
	codeLastOwner          = "last_owner"
	codePeriodOverlap      = "period_overlap"
)

type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Field   string `json:"field,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorBody{Error: errorDetail{Code: code, Message: message}})
}

// fieldError is a validation error of one request field.
type fieldError struct {
	Field   string
	Message string
}

func (e *fieldError) Error() string { return e.Field + ": " + e.Message }

func invalid(field, message string) *fieldError {
	return &fieldError{Field: field, Message: message}
}

func writeInvalid(w http.ResponseWriter, err *fieldError) {
	writeJSON(w, http.StatusBadRequest, errorBody{Error: errorDetail{
		Code: codeValidation, Message: err.Message, Field: err.Field,
	}})
}

// decode reads a single JSON object with unknown fields rejected.
func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var tooLarge *http.MaxBytesError
		switch {
		case errors.As(err, &tooLarge):
			writeError(w, http.StatusRequestEntityTooLarge, codeInvalidJSON, "request body is too large")
		case errors.Is(err, io.EOF):
			writeError(w, http.StatusBadRequest, codeInvalidJSON, "request body is empty")
		default:
			writeError(w, http.StatusBadRequest, codeInvalidJSON, "invalid JSON: "+err.Error())
		}
		return false
	}
	if dec.More() {
		writeError(w, http.StatusBadRequest, codeInvalidJSON, "request body must be a single JSON object")
		return false
	}
	return true
}

// decodeOptional is decode for endpoints where the body may be omitted.
func decodeOptional(w http.ResponseWriter, r *http.Request, dst any) bool {
	if r.ContentLength == 0 {
		return true
	}
	return decode(w, r, dst)
}
