package index

import (
	"context"
	"strings"

	"github.com/ttab/elephantine"
	"github.com/twitchtv/twirp"
)

const (
	ScopeIndexAdmin = "index_admin"
)

func RequireAnyScope(ctx context.Context, scopes ...string) (*elephantine.AuthInfo, error) {
	auth, ok := elephantine.GetAuthInfo(ctx)
	if !ok {
		return nil, twirp.Unauthenticated.Error(
			"no anonymous access allowed")
	}

	if !auth.Claims.HasAnyScope(scopes...) {
		return nil, twirp.PermissionDenied.Errorf(
			"one of the the scopes %s is required",
			strings.Join(scopes, ", "))
	}

	return auth, nil
}
