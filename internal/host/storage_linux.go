package host

import (
	"errors"
	"os"
	"syscall"
	"time"
)

// ReadStorage inspects the filesystem of an existing directory without writing.
func ReadStorage(path string) (Storage, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC, 0)
	if err != nil {
		return Storage{}, &os.PathError{Op: "open directory", Path: path, Err: err}
	}
	defer syscall.Close(fd)
	var st syscall.Statfs_t
	if err := syscall.Fstatfs(fd, &st); err != nil {
		return Storage{}, &os.PathError{Op: "statfs", Path: path, Err: err}
	}
	size := st.Frsize
	if size == 0 {
		size = st.Bsize
	}
	if size <= 0 {
		return Storage{}, errors.New("invalid filesystem block size")
	}
	return storageFromBlocks(path, uint64(size), st.Blocks, st.Bavail, time.Now())
}
