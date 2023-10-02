package scroll

// RollerError is used for Roller sentinel errors.
type RollerError string

func (err RollerError) Error() string {
	return string(err)
}

// Roller sentinel errors.
const (
	// ErrRollerStopped is not an actual error and is only returned if the
	// roller is explicitly stopped by calling Roller.Stop().
	ErrRollerStopped RollerError = "roller was stopped"
)
