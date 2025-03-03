package postgres

import "github.com/ttab/newsdoc"

type PercolatorDocument struct {
	ID       int64
	Fields   map[string][]string
	Document *newsdoc.Document
}

type SubscriptionSpec struct {
	Source        bool
	Fields        []string
	LoadDocuments bool
}
