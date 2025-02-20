package index

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ttab/elephant-api/newsdoc"
)

type PercolatorSpec struct {
	Targets []PercolatorTarget
	Query   map[string]any
}

type PercolatorTarget struct {
	Type     string
	Language string
}

type PercolatorSubscription struct {
	Fields       []string
	Source       bool
	LoadDocument bool
}

func NewPercolator(db *pgxpool.Pool) {
}

type Percolator struct{}

func (p *Percolator) PercolateDocument(
	index string, document map[string][]string, doc *newsdoc.Document,
) {
}
