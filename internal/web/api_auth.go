package web

import (
	"net/http"
	"time"
)

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type sessionInfo struct {
	Authenticated bool   `json:"authenticated"`
	Username      string `json:"username,omitempty"`
	ExpiresAt     string `json:"expires_at,omitempty"`
	BasePath      string `json:"base_path,omitempty"`
	ServerName    string `json:"server_name,omitempty"`
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	st := s.rclone.Status()
	writeJSON(w, http.StatusOK, map[string]any{
		"status":       "ok",
		"time":         time.Now().UTC().Format(time.RFC3339),
		"rclone_ready": st.Ready,
		"rclone_state": st.State,
	})
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	ip := clientIP(r, s.cfg.Server.TrustedProxy)
	if !s.limiter.allow(ip) {
		s.logger.Warn("登录尝试被限流", "ip", ip)
		writeErr(w, http.StatusTooManyRequests, "rate_limited", "登录失败次数过多，请稍后再试")
		return
	}

	var req loginRequest
	if err := decodeJSON(r, &req); err != nil {
		writeStoreErr(w, err)
		return
	}
	if !s.auth.checkCredentials(req.Username, req.Password) {
		s.limiter.fail(ip)
		s.logger.Warn("登录失败", "username", req.Username, "ip", ip)
		writeErr(w, http.StatusUnauthorized, "bad_credentials", "用户名或密码错误")
		return
	}

	token, exp, err := s.auth.issue()
	if err != nil {
		s.logger.Error("生成会话失败", "error", err.Error())
		writeErr(w, http.StatusInternalServerError, "internal", "生成会话失败")
		return
	}
	s.auth.setCookie(w, r, token, exp)
	s.limiter.reset(ip)

	s.logger.Info("登录成功", "username", req.Username, "ip", ip, "expires_at", exp.Format(time.RFC3339))
	writeJSON(w, http.StatusOK, sessionInfo{
		Authenticated: true,
		Username:      req.Username,
		ExpiresAt:     exp.Format(time.RFC3339),
		BasePath:      s.cfg.Server.BasePath,
		ServerName:    "CloudSync Manager",
	})
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	s.auth.clearCookie(w, r)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleMe(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie(sessionCookie)
	if err != nil || s.auth.verify(cookie.Value) != nil {
		writeJSON(w, http.StatusOK, sessionInfo{
			Authenticated: false,
			BasePath:      s.cfg.Server.BasePath,
			ServerName:    "CloudSync Manager",
		})
		return
	}
	writeJSON(w, http.StatusOK, sessionInfo{
		Authenticated: true,
		Username:      s.cfg.Server.Username,
		BasePath:      s.cfg.Server.BasePath,
		ServerName:    "CloudSync Manager",
	})
}
