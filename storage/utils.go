package storage

import "encoding/binary"

func Uint64ToBytes(i uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], i)
	return buf[:]
}

func BytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}
