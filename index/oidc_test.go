package index_test

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/ttab/eltest"
)

type ElsinodConfig struct {
	PublicURL string
}

func NewElsinod(t T, conf ElsinodConfig) *Elsinod {
	pg, err := eltest.BootstrapService("elsinod", &Elsinod{
		conf: conf,
	}, t)
	eltest.Must(t, err, "bootstrap elsinod")

	return pg
}

type Elsinod struct {
	conf ElsinodConfig
	res  *dockertest.Resource
}

func (e *Elsinod) GetAPIEndpoint() string {
	return fmt.Sprintf("http://localhost:%s/.well-known/openid-configuration",
		e.res.GetPort("1080/tcp"))
}

func (e *Elsinod) GetContainerAPIEndpoint() string {
	hostname := strings.TrimPrefix(e.res.Container.Name, "/")

	return fmt.Sprintf("http://%s:1080/.well-known/openid-configuration",
		hostname)
}

func (e *Elsinod) SetUp(pool *dockertest.Pool, network *dockertest.Network) error {
	res, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "ghcr.io/ttab/elsinod",
		Tag:        "v0.2.0-pre2",
		Cmd:        []string{"mock"},
		Env: []string{
			fmt.Sprintf("PUBLIC_URL=%s", e.conf.PublicURL),
			"CLIENT_SECRET=pass",
			"DEMO_PASSWORD=pass",
			"ORGANISATION=example",
		},
		NetworkID: network.Network.ID,
	}, func(hc *docker.HostConfig) {
		hc.AutoRemove = true
	})
	if err != nil {
		return fmt.Errorf("create container: %w", err)
	}

	e.res = res

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
		return fmt.Errorf("failed to connect to elsinod: %w", err)
	}

	return nil
}

func (e *Elsinod) Purge(pool *dockertest.Pool) error {
	if e.res == nil {
		return nil
	}

	err := pool.Purge(e.res)
	if err != nil {
		return fmt.Errorf(
			"failed to purge elsinod container: %w", err)
	}

	return nil
}
