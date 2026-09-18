package web

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"cloudsync/internal/logging"
	"cloudsync/internal/store"
)

// apiError 是统一的错误响应体。
type apiError struct {
	Error string `json:"error"`
	Code  string `json:"code,omitempty"`
	Hint  string `json:"hint,omitempty"`
}

// JSON 写入辅助。

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	if body == nil {
		return
	}
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(body); err != nil {
		// 响应已开始写出，无法再改状态码，只能记录。
		slog.Default().Debug("写入 JSON 响应失败", logging.Err(err))
	}
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, apiError{Error: msg, Code: code})
}

// writeStoreErr 把存储层错误映射为合适的状态码。
func writeStoreErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeErr(w, http.StatusNotFound, "not_found", err.Error())
	case errors.Is(err, store.ErrConflict):
		writeErr(w, http.StatusConflict, "conflict", err.Error())
	default:
		var ve *validationError
		if errors.As(err, &ve) {
			writeErr(w, http.StatusBadRequest, "validation_failed", ve.Error())
			return
		}
		writeErr(w, http.StatusInternalServerError, "internal", err.Error())
	}
}

// validationError 表示入参校验失败。
type validationError struct{ msg string }

func (e *validationError) Error() string { return e.msg }

func invalid(format string, args ...any) error {
	return &validationError{msg: fmt.Sprintf(format, args...)}
}

// decodeJSON 解析请求体，限制体积并拒绝未知字段拼写错误带来的静默失败。
func decodeJSON(r *http.Request, dst any) error {
	defer func() { _, _ = io.Copy(io.Discard, io.LimitReader(r.Body, 1<<20)) }()
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return invalid("读取请求体失败: %v", err)
	}
	if len(strings.TrimSpace(string(body))) == 0 {
		return invalid("请求体不能为空")
	}
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return invalid("请求体格式错误: %v", err)
	}
	return nil
}

// pathID 从路径参数中解析 int64 ID。
func pathID(r *http.Request, name string) (int64, error) {
	raw := r.PathValue(name)
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		return 0, invalid("无效的 %s: %q", name, raw)
	}
	return id, nil
}

// queryInt 读取整型查询参数。
func queryInt(r *http.Request, name string, def int) int {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	return n
}

// queryBool 读取布尔查询参数。
func queryBool(r *http.Request, name string) bool {
	switch strings.ToLower(r.URL.Query().Get(name)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}
