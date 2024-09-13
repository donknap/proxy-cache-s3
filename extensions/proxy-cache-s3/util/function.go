package util

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
)

func CalculateTTL(ttl string) int {
	ttl = strings.Replace(ttl, " ", "", -1)
	if strings.HasSuffix(ttl, "s") || strings.HasSuffix(ttl, "S") {
		ttl = strings.TrimSuffix(ttl, "s")
		ttl = strings.TrimSuffix(ttl, "S")
		ttlInt, err := strconv.Atoi(ttl)
		if err != nil {
			return 0
		}
		return ttlInt
	}
	if strings.HasSuffix(ttl, "m") || strings.HasSuffix(ttl, "M") {
		ttl = strings.TrimSuffix(ttl, "m")
		ttl = strings.TrimSuffix(ttl, "M")
		ttlInt, err := strconv.Atoi(ttl)
		if err != nil {
			return 0
		}
		return ttlInt * 60
	}
	if strings.HasSuffix(ttl, "h") || strings.HasSuffix(ttl, "H") {
		ttl = strings.TrimSuffix(ttl, "h")
		ttl = strings.TrimSuffix(ttl, "H")
		ttlInt, err := strconv.Atoi(ttl)
		if err != nil {
			return 0
		}
		return ttlInt * 60 * 60
	}
	if strings.HasSuffix(ttl, "d") || strings.HasSuffix(ttl, "D") {
		ttl = strings.TrimSuffix(ttl, "d")
		ttl = strings.TrimSuffix(ttl, "D")
		ttlInt, err := strconv.Atoi(ttl)
		if err != nil {
			return 0
		}
		return ttlInt * 60 * 60 * 24
	}
	ttlInt, err := strconv.Atoi(ttl)
	if err != nil {
		return 0
	}
	return ttlInt
}

func GetCacheKey(str string) string {
	hasher := sha256.New()
	hasher.Write([]byte(str))
	hashBytes := hasher.Sum(nil)
	hashString := hex.EncodeToString(hashBytes)
	return hashString
}

func InArray[T uint32 | string](v []T, item T) bool {
	if v == nil {
		return false
	}
	for _, t := range v {
		if t == item {
			return true
		}
	}
	return false
}

func GetPath(cacheKey []string) string {
	path := make([]string, 0)
	key := strings.Split(GetCacheKey(strings.Join(cacheKey, "-")), "")
	for i := 0; i < len(key); i += 2 {
		path = append(path, strings.Join(key[i:i+2], ""))
	}
	return strings.Join(path, "/")
}
