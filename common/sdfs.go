package common

import (
	"errors"
	"fmt"
	"hash/fnv"
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
	Lock       sync.RWMutex
}

const (
	SDFS_REPLICA_NUM    = 4
	SDFS_REPLICA_QUORUM = 3
)

var (
	SDFSFileVersionSequenceMap    map[string]int = make(map[string]int)
	SDFSFileVersionSequenceMapMux sync.RWMutex

	SDFSFileInfoMap    map[string]*SDFSFileInfo = make(map[string]*SDFSFileInfo)
	SDFSFileInfoMapMux sync.RWMutex
)

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
	val, ok := SDFSFileInfoMap[filename]
	SDFSFileInfoMapMux.RUnlock()

	if !ok {
		return 0
	}

	val.Lock.RLock()
	defer val.Lock.RUnlock()

	return val.Version
}

func SDFSUpdateFileVersion(filename string) int {
	SDFSFileVersionSequenceMapMux.Lock()
	defer SDFSFileVersionSequenceMapMux.Unlock()

	if _, ok := SDFSFileVersionSequenceMap[filename]; !ok {
		SDFSFileVersionSequenceMap[filename] = SDFSGetCurrentVersion(filename) + 100
	}

	SDFSFileVersionSequenceMap[filename] += 1
	fmt.Printf("UpdateFileVersion file %v version %v\n", filename, SDFSFileVersionSequenceMap[filename])
	return SDFSFileVersionSequenceMap[filename]
}

func SDFSUpdateFile(filename string, deleteFlag bool, version int, length int, content []byte) {
	SDFSFileInfoMapMux.Lock()
	val, ok := SDFSFileInfoMap[filename]
	if !ok {
		val = &SDFSFileInfo{
			filename,
			SDFSHash(filename),
			time.Now(),
			0,
			false,
			sync.RWMutex{},
		}
		SDFSFileInfoMap[filename] = val
	}
	SDFSFileInfoMapMux.Unlock()

	val.Lock.Lock()
	defer val.Lock.Unlock()

	if val.Version >= version {
		// current version is up-to-date or newer, no need to update
		return
	}
	val.Timestamp = time.Now()
	val.Version = version
	val.DeleteFlag = deleteFlag

	fmt.Printf("Update %v, version %v, delete %v\n", filename, version, deleteFlag)
	if !deleteFlag {
		WriteFile(SDFSPath(filename), content[0:length])
	} else {
		DeleteFile(SDFSPath(filename))
	}
	return
}

func SDFSReadFile(filename string) (bool, int, int, []byte, error) {
	SDFSFileInfoMapMux.Lock()
	val, ok := SDFSFileInfoMap[filename]
	SDFSFileInfoMapMux.Unlock()

	if !ok {
		return true, -1, 0, nil, errors.New("file not found")
	}

	val.Lock.Lock()
	defer val.Lock.Unlock()

	if val.DeleteFlag {
		return true, val.Version, 0, nil, nil
	}

	content, err := ReadFile(SDFSPath(filename))
	if err != nil {
		return true, -1, 0, nil, errors.New("read file error")
	}

	return false, val.Version, len(content), content, nil
}
