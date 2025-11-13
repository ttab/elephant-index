package index_test

import (
	"fmt"
	"net/http"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/ttab/eltest"
)

type RepositoryConfig struct {
	ConnStr       string
	S3Endpoint    string
	ArchiveBucket string
	AssetBucket   string
	OIDCConfig    string
}

func NewRepository(t T, conf RepositoryConfig) *Repository {
	pg, err := eltest.BootstrapService("repository", &Repository{
		conf: conf,
	}, t)
	eltest.Must(t, err, "bootstrap repository")

	return pg
}

type Repository struct {
	conf RepositoryConfig
	res  *dockertest.Resource
}

func (r *Repository) GetAPIEndpoint() string {
	return fmt.Sprintf("http://localhost:%s",
		r.res.GetPort("1080/tcp"))
}

func (r *Repository) SetUp(pool *dockertest.Pool, network *dockertest.Network) error {
	res, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "ghcr.io/ttab/elephant-repository",
		Tag:        "v1.2.5",
		Cmd:        []string{"run"},
		Env: []string{
			"NO_EVENTSINK=true",
			"MIGRATE_DB=true",
			fmt.Sprintf("CONN_STRING=%s", r.conf.ConnStr),
			fmt.Sprintf("S3_ENDPOINT=%s", r.conf.S3Endpoint),
			"S3_ACCESS_KEY_ID=minioadmin",
			"S3_ACCESS_KEY_SECRET=minioadmin",
			fmt.Sprintf("ARCHIVE_BUCKET=%s", r.conf.ArchiveBucket),
			fmt.Sprintf("ASSET_BUCKET=%s", r.conf.AssetBucket),
			fmt.Sprintf("OIDC_CONFIG=%s", r.conf.OIDCConfig),
		},
		NetworkID: network.Network.ID,
	}, func(hc *docker.HostConfig) {
		hc.AutoRemove = true
	})
	if err != nil {
		return fmt.Errorf("create container: %w", err)
	}

	r.res = res

	// Make sure that containers don't stick around for more than an hour,
	// even if in-process cleanup fails.
	_ = res.Expire(3600)

	err = pool.Retry(func() error {
		readyEndpoint := fmt.Sprintf("http://localhost:%s/health/ready",
			res.GetPort("1081/tcp"))

		res, err := http.Get(readyEndpoint) //nolint: gosec
		if err != nil {
			return fmt.Errorf("do readiness check: %w", err)
		}

		// TODO: read response data
		_ = res.Body.Close()

		if res.StatusCode != http.StatusOK {
			return fmt.Errorf("not ready: %s", res.Status)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to connect to repository: %w", err)
	}

	return nil
}

func (r *Repository) Purge(pool *dockertest.Pool) error {
	if r.res == nil {
		return nil
	}

	err := pool.Purge(r.res)
	if err != nil {
		return fmt.Errorf(
			"failed to purge repository container: %w", err)
	}

	return nil
}
