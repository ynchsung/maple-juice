package common

import (
	"hash/fnv"
	"math/rand"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

type SDFSFileInfo struct {
	Filename   string
	Timestamp  time.Time
	Version    int
	DeleteFlag bool
}

const (
	SDFS_REPLICA_NUM    = 4
	SDFS_REPLICA_QUORUM = 3
)

var (
	SDFSRequestsAckCounterMap    map[string]int
	SDFSRequestsAckCounterMapMux sync.RWMutex
)

func SDFSGenerateRequestToken() string {
	const (
		l   string = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
		LEN        = 16
	)

	var ret string = ""
	for i := 0; i < LEN; i++ {
		ret += string(l[rand.Intn(len(l))])
	}

	return ret
}

func SDFSHash(filename string) int {
	const M uint32 = 11

	h := fnv.New32a()
	h.Write([]byte(filename))
	return int(h.Sum32() % M)
}

func SDFSPath(filename string) string {
	r := regexp.MustCompile("/")
	return filepath.Join(Cfg.SDFSDir, r.ReplaceAllString(filename, "_"))
}

func SDFSPutFile(filename string, length int, content []byte) error {
	_, err := WriteFile(SDFSPath(filename), content[0:length])
	return err
}
