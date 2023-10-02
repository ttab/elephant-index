package scroll

import "context"

// StateStore can be implemented by any backend that persists the log position
// state.
//
// The Roller will call SetState() after each batch has finished processing and
// when it transitions between the compacted and streaming eventlog.
type StateStore interface {
	GetState(ctx context.Context) (*State, error)
	SetState(ctx context.Context, state *State) error
}

type State struct {
	// Position in the eventlog.
	Position int64 `json:"position"`
	// Streaming is true if we have caught up with the eventlog and are
	// reading the uncompacted log.
	Streaming bool `json:"streaming"`
}
