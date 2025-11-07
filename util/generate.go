package util

import "hash/fnv"

func GenerateID(payload string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(payload))
	return h.Sum64()
}
