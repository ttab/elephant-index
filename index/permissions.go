package index

import (
	"context"
	"strings"

	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/rpc"
)

const (
	ScopeIndexAdmin = "index_admin"
	ScopeSearch     = "search"
)

// HTTP header names.
const (
	// headerAuthorization carries the bearer token.
	headerAuthorization = "Authorization"
	headerContentType   = "Content-Type"
)

func RequireAnyScope(ctx context.Context, scopes ...string) (*elephantine.AuthInfo, error) {
	auth, ok := elephantine.GetAuthInfo(ctx)
	if !ok {
		return nil, rpc.Unauthenticated(
			"no anonymous access allowed")
	}

	if !auth.Claims.HasAnyScope(scopes...) {
		return nil, rpc.PermissionDeniedf(
			"one of the the scopes %s is required",
			strings.Join(scopes, ", "))
	}

	return auth, nil
}
