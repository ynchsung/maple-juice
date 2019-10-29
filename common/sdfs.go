package common

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

type SDFSFileInfo struct {
	Filename   string
	Key        int
	Timestamp  time.Time
	Version    int
	DeleteFlag bool
}

const (
	SDFS_REPLICA_NUM    = 4
	SDFS_REPLICA_QUORUM = 3
)

var (
	/*
		SDFSRequestsAckCounterMap    map[string]int
		SDFSRequestsAckCounterMapMux sync.RWMutex
	*/

	SDFSFileVersionSequenceMap    map[string]int = make(map[string]int)
	SDFSFileVersionSequenceMapMux sync.RWMutex

	SDFSFileInfoMap    map[string]*SDFSFileInfo = make(map[string]*SDFSFileInfo)
	SDFSFileInfoMapMux sync.RWMutex
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

func SDFSGetCurrentVersion(filename string) int {
	SDFSFileInfoMapMux.RLock()
	defer SDFSFileInfoMapMux.RUnlock()

	if _, ok := SDFSFileInfoMap[filename]; !ok {
		return 0
	}
	return SDFSFileInfoMap[filename].Version
}

func SDFSUpdateFileVersion(filename string) int {
	SDFSFileVersionSequenceMapMux.Lock()
	defer SDFSFileVersionSequenceMapMux.Unlock()

	if _, ok := SDFSFileVersionSequenceMap[filename]; !ok {
		SDFSFileVersionSequenceMap[filename] = SDFSGetCurrentVersion(filename)
	}

	SDFSFileVersionSequenceMap[filename] += 1
	fmt.Printf("UpdateFileVersion file %v version %v\n", filename, SDFSFileVersionSequenceMap[filename])
	return SDFSFileVersionSequenceMap[filename]
}

func SDFSPutFile(filename string, version int, length int, content []byte) {
	SDFSFileInfoMapMux.Lock()
	defer SDFSFileInfoMapMux.Unlock()

	val, ok := SDFSFileInfoMap[filename]
	if !ok {
		SDFSFileInfoMap[filename] = &SDFSFileInfo{
			filename,
			SDFSHash(filename),
			time.Now(),
			version,
			false,
		}
	} else {
		if val.Version >= version {
			// current version is up-to-date or newer, no need to update
			return
		}
		val.Timestamp = time.Now()
		val.Version = version
		val.DeleteFlag = false
	}

	fmt.Printf("PutFile %v version %v\n", filename, version)

	WriteFile(SDFSPath(filename), content[0:length])
	return
}
