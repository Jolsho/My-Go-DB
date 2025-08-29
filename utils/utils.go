package utils

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
)

const BUFFER_SIZE = 1024

func RandomInt32() (int32, error) {
	buf := make([]byte, 4)
	n, err := rand.Read(buf)
	if n != 4 {
		return 0, errors.New("Rand Read ZERO")
	}
	return int32(binary.LittleEndian.Uint32(buf)), err
}
