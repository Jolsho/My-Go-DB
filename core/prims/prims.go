package prims

import (
	"errors"
	"syscall"
)

func Sync(fd int) (error) {
	if fd < 0 { return errors.New("File not open") }
	return syscall.Fsync(fd)
}

func Write(fd int, buff []byte, offset int64) (int, error) {
	if fd < 0 { return 0, errors.New("File not open") }
	return syscall.Pwrite(fd, buff, offset)
}

func Read(fd int, buff []byte, offset int64) (int, error) {
	if fd < 0 { return 0, errors.New("File not open") }
	return syscall.Pread(fd, buff, offset)
}
