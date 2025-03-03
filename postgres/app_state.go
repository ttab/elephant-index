package postgres

type AppStateData struct {
	Percolator *PercolatorState `json:",omitempty"`
}

type PercolatorState struct {
	CurrentPosition int64
}
