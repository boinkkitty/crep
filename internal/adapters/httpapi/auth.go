// Package httpapi exposes the Task API and Job API over HTTP/JSON.
package httpapi

import (
	"context"
	"crypto"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Authenticator extracts the tenant identity from a request.
type Authenticator interface {
	Authenticate(r *http.Request) (tenantID string, err error)
}

var errUnauthenticated = errors.New("unauthenticated")

// JWTAuth validates bearer tokens and reads the tenant from the configured claim.
type JWTAuth struct {
	keyFunc  jwt.Keyfunc
	methods  []string
	issuer   string
	audience string
	claim    string
}

type JWTConfig struct {
	HMACSecret []byte           // HS256
	PublicKey  crypto.PublicKey // RS256/ES256 (takes precedence when set)
	Issuer     string
	Audience   string
	Claim      string // defaults to "tenant_id"
}

func NewJWTAuth(c JWTConfig) (*JWTAuth, error) {
	a := &JWTAuth{issuer: c.Issuer, audience: c.Audience, claim: c.Claim}
	if a.claim == "" {
		a.claim = "tenant_id"
	}
	switch {
	case c.PublicKey != nil:
		a.methods = []string{"RS256", "RS384", "RS512", "ES256", "ES384"}
		a.keyFunc = func(*jwt.Token) (any, error) { return c.PublicKey, nil }
	case len(c.HMACSecret) >= 32:
		a.methods = []string{"HS256"}
		a.keyFunc = func(*jwt.Token) (any, error) { return c.HMACSecret, nil }
	default:
		return nil, errors.New("jwt: configure a public key or an HMAC secret of at least 32 bytes")
	}
	return a, nil
}

func (a *JWTAuth) Authenticate(r *http.Request) (string, error) {
	h := r.Header.Get("Authorization")
	raw, ok := strings.CutPrefix(h, "Bearer ")
	if !ok || raw == "" {
		return "", errUnauthenticated
	}
	opts := []jwt.ParserOption{jwt.WithValidMethods(a.methods), jwt.WithExpirationRequired(), jwt.WithLeeway(30 * time.Second)}
	if a.issuer != "" {
		opts = append(opts, jwt.WithIssuer(a.issuer))
	}
	if a.audience != "" {
		opts = append(opts, jwt.WithAudience(a.audience))
	}
	claims := jwt.MapClaims{}
	if _, err := jwt.ParseWithClaims(raw, claims, a.keyFunc, opts...); err != nil {
		return "", errUnauthenticated
	}
	tenant, _ := claims[a.claim].(string)
	if tenant == "" || len(tenant) > 128 {
		return "", errUnauthenticated
	}
	return tenant, nil
}

type tenantKey struct{}

func tenantFrom(ctx context.Context) string { s, _ := ctx.Value(tenantKey{}).(string); return s }

func requireAuth(a Authenticator, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenant, err := a.Authenticate(r)
		if err != nil {
			writeError(w, http.StatusUnauthorized, "unauthenticated", "valid bearer token required")
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), tenantKey{}, tenant)))
	})
}
