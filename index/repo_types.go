package index

import (
	"time"

	"github.com/ttab/newsdoc"
)

type Status struct {
	ID      int64
	Version int64
	Creator string
	Created time.Time
	Meta    newsdoc.DataMap
}

type ACLEntry struct {
	URI         string
	Permissions []string
}
