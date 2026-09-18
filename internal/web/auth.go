package web

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// sessionCookie 是会话 Cookie 名称。
const sessionCookie = "cloudsync_session"

// errUnauthorized 表示会话无效或凭据错误。
var errUnauthorized = errors.New("未认证或会话已过期")

// auth 负责管理端认证。
//
// 会话令牌为自包含的 HMAC 签名串：base64(nonce):exp:signature。
// 不依赖外部存储，进程重启后（secret 随机生成的情况下）所有会话失效，
// 这是可接受的取舍——如需持久会话，请在配置中固定 server.session_secret。
type auth struct {
	user   string
	pass   string
	secret []byte
	ttl    time.Duration
}

func newAuth(user, pass, secret string, ttl time.Duration) *auth {
	if ttl <= 0 {
		ttl = 12 * time.Hour
	}
	return &auth{
		user:   user,
		pass:   pass,
		secret: []byte(secret),
		ttl:    ttl,
	}
}

// checkCredentials 以恒定时间比较用户名与密码。
func (a *auth) checkCredentials(user, pass string) bool {
	uOK := subtle.ConstantTimeCompare([]byte(user), []byte(a.user)) == 1
	pOK := subtle.ConstantTimeCompare([]byte(pass), []byte(a.pass)) == 1
	return uOK && pOK
}

// issue 生成会话令牌。
func (a *auth) issue() (string, time.Time, error) {
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return "", time.Time{}, fmt.Errorf("生成会话随机数: %w", err)
	}
	exp := time.Now().Add(a.ttl)
	payload := hex.EncodeToString(nonce) + ":" + strconv.FormatInt(exp.Unix(), 10)
	return payload + ":" + a.sign(payload), exp, nil
}

// verify 校验会话令牌。
func (a *auth) verify(token string) error {
	if token == "" {
		return errUnauthorized
	}
	idx := strings.LastIndexByte(token, ':')
	if idx <= 0 {
		return errUnauthorized
	}
	payload, sig := token[:idx], token[idx+1:]
	if !hmac.Equal([]byte(sig), []byte(a.sign(payload))) {
		return errUnauthorized
	}
	parts := strings.Split(payload, ":")
	if len(parts) != 2 {
		return errUnauthorized
	}
	exp, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return errUnauthorized
	}
	if time.Now().After(time.Unix(exp, 0)) {
		return fmt.Errorf("%w: 会话已过期，请重新登录", errUnauthorized)
	}
	return nil
}

func (a *auth) sign(payload string) string {
	mac := hmac.New(sha256.New, a.secret)
	mac.Write([]byte(payload))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// setCookie 下发会话 Cookie。
func (a *auth) setCookie(w http.ResponseWriter, r *http.Request, token string, exp time.Time) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    token,
		Path:     "/",
		Expires:  exp,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   requestIsTLS(r),
	})
}

// clearCookie 清除会话 Cookie。
func (a *auth) clearCookie(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   requestIsTLS(r),
	})
}

func requestIsTLS(r *http.Request) bool {
	return r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https")
}

// clientIP 返回客户端 IP，trustProxy 为 true 时采信 X-Forwarded-For。
func clientIP(r *http.Request, trustProxy bool) string {
	if trustProxy {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			parts := strings.Split(xff, ",")
			if ip := strings.TrimSpace(parts[0]); ip != "" {
				return ip
			}
		}
		if xr := r.Header.Get("X-Real-Ip"); xr != "" {
			return strings.TrimSpace(xr)
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
