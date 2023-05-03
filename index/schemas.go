package index

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/ttab/elephant/revisor"
	"github.com/ttab/elephant/rpc/repository"
	"github.com/ttab/elephantine"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slog"
)

type SchemaLoader struct {
	logger *slog.Logger
	client repository.Schemas

	m             sync.RWMutex
	v             *revisor.Validator
	knownVersions map[string]string
}

func NewSchemaLoader(
	ctx context.Context,
	logger *slog.Logger,
	client repository.Schemas,
) (*SchemaLoader, error) {
	sl := SchemaLoader{
		logger: logger,
		client: client,
	}

	err := sl.loadSchemas(ctx)
	if err != nil {
		return nil, fmt.Errorf("initial schema load: %w", err)
	}

	go sl.loadLoop(ctx)

	return &sl, nil
}

func (sl *SchemaLoader) GetValidator() *revisor.Validator {
	sl.m.RLock()
	defer sl.m.RUnlock()

	return sl.v
}

func (sl *SchemaLoader) loadLoop(ctx context.Context) {
	for {
		t := time.Now()

		err := sl.loadSchemas(ctx)
		if err != nil {
			sl.logger.ErrorCtx(ctx, "failed to load schemas",
				elephantine.LogKeyError, err)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Until(t.Add(5 * time.Second))):
			// This is done to ensure that we don't start spamming
			// the server in case something goes awry. During normal
			// operations we should land naturally at 1req/10sec
			// unless the schemas are being constantly updated.
			continue
		}
	}
}

func (sl *SchemaLoader) loadSchemas(ctx context.Context) error {
	results, err := sl.client.GetAllActive(ctx, &repository.GetAllActiveSchemasRequest{
		Known: sl.knownVersions,
	})
	if err != nil {
		return fmt.Errorf("get active schemas from repository: %w", err)
	}

	newKnown := make(map[string]string)

	for _, schema := range results.Schemas {
		newKnown[schema.Name] = schema.Version
	}

	if maps.Equal(sl.knownVersions, newKnown) {
		return nil
	}

	var constraints []revisor.ConstraintSet

	for _, schema := range results.Schemas {
		var spec revisor.ConstraintSet

		err := json.Unmarshal([]byte(schema.Spec), &spec)
		if err != nil {
			return fmt.Errorf(
				"invalid schema spec for %s@%s returned by server: %w",
				schema.Name, schema.Version, err)
		}

		constraints = append(constraints, spec)
	}

	validator, err := revisor.NewValidator(constraints...)
	if err != nil {
		return fmt.Errorf(
			"create validator from repository schemas: %w", err)
	}

	sl.m.Lock()
	sl.knownVersions = newKnown
	sl.v = validator
	sl.m.Unlock()

	return nil
}
