package index

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ttab/elephant-index/postgres"
	"github.com/viccon/sturdyc"
)

func NewPercolatorDocCache(db *pgxpool.Pool) *PercolatorDocCache {
	pdc := PercolatorDocCache{
		q:     postgres.New(db),
		cache: sturdyc.New[postgres.PercolatorDocument](5000, 1, 1*time.Hour, 10),
	}

	return &pdc
}

type PercolatorDocCache struct {
	q     *postgres.Queries
	cache *sturdyc.Client[postgres.PercolatorDocument]
}

func (pdc *PercolatorDocCache) CacheDocument(d postgres.PercolatorDocument) {
	pdc.cache.Set(strconv.FormatInt(d.ID, 10), d)
}

func (pdc *PercolatorDocCache) GetDocument(
	ctx context.Context,
	id int64,
) (postgres.PercolatorDocument, error) {
	key := strconv.FormatInt(id, 10)

	doc, err := pdc.cache.GetOrFetch(ctx, key, func(ctx context.Context) (postgres.PercolatorDocument, error) {
		res, err := pdc.q.GetPercolatorEventPayload(ctx, id)
		if err != nil {
			return postgres.PercolatorDocument{}, fmt.Errorf(
				"load document from database: %w", err)
		}

		return res.Data, nil
	})
	if err != nil {
		return postgres.PercolatorDocument{}, err
	}

	return doc, nil
}
