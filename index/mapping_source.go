package index

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ttab/elephant-index/postgres"
)

func NewPostgresMappingSource(
	q *postgres.Queries,
) *PostgresMappingSource {
	return &PostgresMappingSource{
		q: q,
	}
}

type PostgresMappingSource struct {
	q *postgres.Queries
}

func (pms *PostgresMappingSource) GetMappings(
	ctx context.Context, setName string, contentType string,
) (map[string]Mapping, error) {
	data, err := pms.q.GetMappingsForType(ctx,
		postgres.GetMappingsForTypeParams{
			SetName:     setName,
			ContentType: contentType,
		})
	if err != nil {
		return nil, fmt.Errorf(
			"read type mappings: %w", err)
	}

	mappings := NewMappings()

	for _, md := range data {
		var instance Mappings

		err := json.Unmarshal(md.Mappings, &instance.Properties)
		if err != nil {
			return nil, fmt.Errorf(
				"unmarshal %q mappings: %w", md.Name, err)
		}

		changes := instance.ChangesFrom(mappings)
		mappings = changes.Superset(mappings)
	}

	return mappings.Properties, nil
}
