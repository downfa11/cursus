package util

import "hash/fnv"

// Hash returns a uint32 hash of the given string key.
func Hash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32())
}

func GenerateID(payload string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(payload))
	return h.Sum64()
}
