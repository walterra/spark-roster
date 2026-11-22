//go:build !linux

package host

import "errors"

// ReadStorage requires a local Linux filesystem.
func ReadStorage(string) (Storage, error) {
	return Storage{}, errors.New("live storage inspection requires Linux")
}
