package index

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/url"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lucasepe/codename"
	"github.com/ttab/elephant-api/index"
	"github.com/ttab/elephant-index/postgres"
	"github.com/ttab/elephantine/pg"
	"github.com/twitchtv/twirp"
)

type ManagementService struct {
	logger  *slog.Logger
	db      *pgxpool.Pool
	nameRng *rand.Rand
}

func NewManagementService(
	logger *slog.Logger,
	db *pgxpool.Pool,
) (*ManagementService, error) {
	rng, err := codename.DefaultRNG()
	if err != nil {
		return nil, fmt.Errorf("initialise index name RNG: %w", err)
	}

	return &ManagementService{
		logger:  logger,
		db:      db,
		nameRng: rng,
	}, nil
}

// DeleteIndexSet implements index.Management.
func (s *ManagementService) DeleteIndexSet(
	ctx context.Context, req *index.DeleteIndexSetRequest,
) (*index.DeleteIndexSetResponse, error) {
	_, err := RequireAnyScope(ctx, ScopeIndexAdmin)
	if err != nil {
		return nil, err
	}

	var twErr twirp.Error

	err = pg.WithTX(ctx, s.db, func(tx pgx.Tx) error {
		q := postgres.New(tx)

		idx, err := q.GetIndexSetForUpdate(ctx, req.Name)
		if errors.Is(err, pgx.ErrNoRows) || idx.Deleted {
			return nil
		}

		if idx.Active || idx.Enabled {
			return twirp.FailedPrecondition.Error(
				"active or enabled index sets cannot be deleted",
			)
		}

		err = s.updateIndexSetStatus(ctx, tx, q,
			postgres.SetIndexSetStatusParams{
				Name:    idx.Name,
				Enabled: false,
				Active:  false,
				Deleted: true,
			})
		if err != nil {
			return err
		}

		return nil
	})
	if ok := errors.As(err, &twErr); ok {
		return nil, twErr
	} else if err != nil {
		return nil, twirp.InternalErrorf(
			"database operation failed: %v", err,
		)
	}

	return &index.DeleteIndexSetResponse{}, nil
}

func (s *ManagementService) updateIndexSetStatus(
	ctx context.Context,
	tx pgx.Tx,
	q *postgres.Queries,
	params postgres.SetIndexSetStatusParams,
) error {
	err := q.SetIndexSetStatus(ctx, params)
	if err != nil {
		return fmt.Errorf("set status: %w", err)
	}

	err = pg.Publish(ctx, tx, ChanIndexStatusChange,
		IndexStatusChange{
			Name: params.Name,
		})
	if err != nil {
		return fmt.Errorf("publish status change event: %w", err)
	}

	return nil
}

// DeleteCluster implements index.Management.
func (s *ManagementService) DeleteCluster(
	ctx context.Context, req *index.DeleteClusterRequest,
) (*index.DeleteClusterResponse, error) {
	_, err := RequireAnyScope(ctx, ScopeIndexAdmin)
	if err != nil {
		return nil, err
	}

	if req.Name == "" {
		return nil, twirp.RequiredArgumentError("name")
	}

	var twErr twirp.Error

	err = pg.WithTX(ctx, s.db, func(tx pgx.Tx) error {
		return s.deleteCluster(ctx, tx, req.Name)
	})
	if ok := errors.As(err, &twErr); ok {
		return nil, twErr
	} else if err != nil {
		return nil, twirp.InternalErrorf(
			"database operation failed: %v", err,
		)
	}

	return &index.DeleteClusterResponse{}, nil
}

func (s *ManagementService) deleteCluster(
	ctx context.Context, tx pgx.Tx, name string,
) error {
	q := postgres.New(tx)

	cluster, err := q.GetClusterForUpdate(ctx, name)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	} else if err != nil {
		return twirp.InternalErrorf(
			"failed to read cluster information: %v", err,
		)
	}

	counts, err := q.ClusterIndexCount(ctx, cluster.Name)
	if err != nil {
		return fmt.Errorf("get cluster index count: %w", err)
	}

	if counts.PendingDelete > 0 {
		return twirp.FailedPrecondition.Errorf(
			"%d indexes are still pending delete: %w",
			counts.PendingDelete, err)
	} else if counts.Total > 0 {
		return twirp.FailedPrecondition.Error(
			"the cluster still has indexes")
	}

	err = q.DeleteCluster(ctx, cluster.Name)
	if err != nil {
		return fmt.Errorf("delete cluster: %w", err)
	}

	return nil
}

// ListClusters implements index.Management.
func (s *ManagementService) ListClusters(
	ctx context.Context, _ *index.ListClustersRequest,
) (*index.ListClustersResponse, error) {
	_, err := RequireAnyScope(ctx, ScopeIndexAdmin)
	if err != nil {
		return nil, err
	}

	q := postgres.New(s.db)

	rows, err := q.ListClustersWithCounts(ctx)
	if err != nil {
		return nil, twirp.InternalErrorf(
			"failed to read from database: %w", err)
	}

	res := make([]*index.Cluster, len(rows))

	for i, row := range rows {
		var auth ClusterAuth

		err := json.Unmarshal(row.Auth, &auth)
		if err != nil {
			return nil, twirp.InternalErrorf(
				"decode auth information for %q: %w",
				row.Name, err)
		}

		res[i] = &index.Cluster{
			Name:          row.Name,
			Endpoint:      row.Url,
			IndexSetCount: row.IndexSetCount,
			Auth: &index.ClusterAuth{
				Iam: auth.IAM,
			},
		}
	}

	return &index.ListClustersResponse{
		Clusters: res,
	}, nil
}

// ListIndexSets implements index.Management.
func (s *ManagementService) ListIndexSets(
	ctx context.Context, req *index.ListIndexSetsRequest,
) (*index.ListIndexSetsResponse, error) {
	_, err := RequireAnyScope(ctx, ScopeIndexAdmin)
	if err != nil {
		return nil, err
	}

	q := postgres.New(s.db)

	var (
		active  *bool
		enabled *bool
	)

	if req.OnlyActive {
		active = boolP(true)
	}

	switch req.Status {
	case index.EnabledFilter_STATUS_ENABLED:
		enabled = boolP(true)
	case index.EnabledFilter_STATUS_DISABLED:
		enabled = boolP(false)
	case index.EnabledFilter_STATUS_UNSPECIFIED:
	}

	sets, err := q.IndexSetQuery(ctx, postgres.IndexSetQueryParams{
		Cluster: pg.TextOrNull(req.Cluster),
		Active:  pg.PBool(active),
		Enabled: pg.PBool(enabled),
	})
	if err != nil {
		return nil, fmt.Errorf("query database: %w", err)
	}

	res := index.ListIndexSetsResponse{
		IndexSets: make([]*index.IndexSet, len(sets)),
	}

	for i := range sets {
		res.IndexSets[i] = &index.IndexSet{
			Name:     sets[i].Name,
			Cluster:  sets[i].Cluster.String,
			Enabled:  sets[i].Enabled,
			Active:   sets[i].Active,
			Position: sets[i].Position,
		}
	}

	return &res, nil
}

func boolP(v bool) *bool {
	return &v
}

// PartialReindex implements index.Management.
func (s *ManagementService) PartialReindex(
	ctx context.Context, _ *index.PartialReindexRequest,
) (*index.PartialReindexResponse, error) {
	_, err := RequireAnyScope(ctx, ScopeIndexAdmin)
	if err != nil {
		return nil, err
	}

	panic("unimplemented")
}

// RegisterCluster implements index.Management.
func (s *ManagementService) RegisterCluster(
	ctx context.Context, req *index.RegisterClusterRequest,
) (*index.RegisterClusterResponse, error) {
	_, err := RequireAnyScope(ctx, ScopeIndexAdmin)
	if err != nil {
		return nil, err
	}

	if req.Endpoint == "" {
		return nil, twirp.RequiredArgumentError("endpoint")
	}

	if req.Auth == nil {
		return nil, twirp.RequiredArgumentError("auth")
	}

	endpointURL, err := url.Parse(req.Endpoint)
	if err != nil {
		return nil, twirp.InvalidArgumentError("endpoint", err.Error())
	}

	if (endpointURL.Scheme != "http" && endpointURL.Scheme != "https") ||
		endpointURL.Host == "" {
		return nil, twirp.InvalidArgumentError(
			"endpoint", "invalid URL, must be http or https and refer to a host")
	}

	auth := ClusterAuth{
		IAM: req.Auth.Iam,
	}

	authData, err := json.Marshal(&auth)
	if err != nil {
		return nil, twirp.InternalErrorf(
			"failed to marshal cluster auth data: %v", err)
	}

	var twErr twirp.Error

	err = pg.WithTX(ctx, s.db, func(tx pgx.Tx) error {
		q := postgres.New(tx)

		err := q.AddCluster(ctx, postgres.AddClusterParams{
			Name: req.Name,
			Url:  req.Endpoint,
			Auth: authData,
		})
		if err != nil {
			return fmt.Errorf("add cluster: %w", err)
		}

		return nil
	})
	if ok := errors.As(err, &twErr); ok {
		return nil, twErr
	} else if err != nil {
		return nil, twirp.InternalErrorf(
			"database operation failed: %v", err,
		)
	}

	return &index.RegisterClusterResponse{}, nil
}

// Reindex implements index.Management.
func (s *ManagementService) Reindex(
	ctx context.Context, req *index.ReindexRequest,
) (*index.ReindexResponse, error) {
	_, err := RequireAnyScope(ctx, ScopeIndexAdmin)
	if err != nil {
		return nil, err
	}

	if req.Cluster == "" {
		return nil, twirp.RequiredArgumentError("cluster")
	}

	var (
		name  string
		twErr twirp.Error
	)

	err = pg.WithTX(ctx, s.db, func(tx pgx.Tx) error {
		n, err := s.reindex(ctx, tx, req.Cluster)
		if err != nil {
			return err
		}

		name = n

		return nil
	})
	if ok := errors.As(err, &twErr); ok {
		return nil, twErr
	} else if err != nil {
		return nil, twirp.InternalErrorf(
			"database operation failed: %v", err,
		)
	}

	return &index.ReindexResponse{
		Name: name,
	}, nil
}

func (s *ManagementService) reindex(
	ctx context.Context, tx pgx.Tx, clusterName string,
) (string, error) {
	q := postgres.New(tx)

	// Getting cluster for update as we want to serialise all re-index-ops.
	cluster, err := q.GetClusterForUpdate(ctx, clusterName)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", twirp.InvalidArgumentError("cluster", "no such cluster")
	} else if err != nil {
		return "", fmt.Errorf(
			"read cluster information: %w", err,
		)
	}

	var (
		tries int
		name  string
	)

	for {
		tries++

		// Put a bound on our attempts, we are dealing with randomness
		// after all.
		if tries > 10 {
			return "", fmt.Errorf(
				"could not find unique index set name after %d attempts",
				tries-1)
		}

		// Generate a codename, but don't start adding random tokens to
		// the end until the fifth attempt.
		n := codename.Generate(s.nameRng, tries/5)

		exists, err := q.IndexSetExists(ctx, n)
		if err != nil {
			return "", fmt.Errorf(
				"check if index set name is unique: %w", err)
		}

		if exists {
			continue
		}

		name = n

		break
	}

	err = q.CreateIndexSet(ctx, postgres.CreateIndexSetParams{
		Name:     name,
		Position: 0,
		Cluster:  cluster.Name,
		Active:   false,
		Enabled:  true,
	})
	if err != nil {
		return "", fmt.Errorf(
			"create index set in database: %w", err)
	}

	err = pg.Publish(ctx, tx, ChanIndexStatusChange,
		IndexStatusChange{
			Name: name,
		})
	if err != nil {
		return "", fmt.Errorf("publish index status change event: %w", err)
	}

	return name, nil
}

// SetIndexSetStatus implements index.Management.
func (s *ManagementService) SetIndexSetStatus(
	ctx context.Context, req *index.SetIndexSetStatusRequest,
) (*index.SetIndexSetStatusResponse, error) {
	_, err := RequireAnyScope(ctx, ScopeIndexAdmin)
	if err != nil {
		return nil, err
	}

	var twErr twirp.Error

	err = pg.WithTX(ctx, s.db, func(tx pgx.Tx) error {
		return s.setIndexSetStatus(ctx, tx, req)
	})
	if ok := errors.As(err, &twErr); ok {
		return nil, twErr
	} else if err != nil {
		return nil, twirp.InternalErrorf(
			"database operation failed: %v", err,
		)
	}

	return &index.SetIndexSetStatusResponse{}, nil
}

const maxActivationLag = 10

func (s *ManagementService) setIndexSetStatus(
	ctx context.Context,
	tx pgx.Tx,
	req *index.SetIndexSetStatusRequest,
) error {
	q := postgres.New(tx)

	idx, err := q.GetIndexSetForUpdate(ctx, req.Name)
	if errors.Is(err, pgx.ErrNoRows) || idx.Deleted {
		return twirp.NotFoundError("no such index")
	}

	if idx.Active && !req.Active {
		return twirp.InvalidArgument.Error(
			"an index set can only be deactivated by activating another set")
	}

	if req.Active {
		err := s.checkActiveReplaced(ctx, tx, q, idx, req.ForceActive)
		if err != nil {
			return err
		}
	}

	err = s.updateIndexSetStatus(ctx, tx, q,
		postgres.SetIndexSetStatusParams{
			Name:    idx.Name,
			Enabled: req.Enabled,
			Active:  req.Active,
			Deleted: idx.Deleted,
		})
	if err != nil {
		return fmt.Errorf("activate the set: %w", err)
	}

	return nil
}

func (s *ManagementService) checkActiveReplaced(
	ctx context.Context,
	tx pgx.Tx,
	q *postgres.Queries,
	idx postgres.IndexSet,
	forceActive bool,
) error {
	currentActive, err := q.GetCurrentActiveForUpdate(ctx)
	if err != nil {
		return fmt.Errorf(
			"get currently active set: %w", err)
	}

	if currentActive.Name != "" && currentActive.Name != idx.Name {
		lag := currentActive.Position - idx.Position
		if lag > maxActivationLag && !forceActive {
			return twirp.FailedPrecondition.Errorf(
				"the index set lags behind with more than %d events (%d), use force_active to activate anyway",
				maxActivationLag,
				lag,
			)
		}

		err := s.updateIndexSetStatus(ctx, tx, q,
			postgres.SetIndexSetStatusParams{
				Name:    currentActive.Name,
				Enabled: currentActive.Enabled,
				Active:  false,
				Deleted: currentActive.Deleted,
			})
		if err != nil {
			return fmt.Errorf(
				"deactivate currently active set: %w", err)
		}
	}

	return nil
}

var _ index.Management = &ManagementService{}
