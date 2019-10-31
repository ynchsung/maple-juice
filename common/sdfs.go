package common

import (
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

type SDFSVersionSequence struct {
	Version   int
	Timestamp time.Time
}

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
	SDFSFileVersionSequenceMap    map[string]*SDFSVersionSequence = make(map[string]*SDFSVersionSequence)
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

func SDFSUpdateFileVersion(filename string, force bool) (int, bool) {
	SDFSFileVersionSequenceMapMux.Lock()
	defer SDFSFileVersionSequenceMapMux.Unlock()

	newestVersion := SDFSGetCurrentVersion(filename)
	now := time.Now()
	val, ok := SDFSFileVersionSequenceMap[filename]
	if !ok {
		SDFSFileVersionSequenceMap[filename] = &SDFSVersionSequence{
			newestVersion + 100,
			now,
		}
	} else {
		if !force && !now.After(val.Timestamp.Add(60*time.Second)) {
			return -1, false
		}

		if val.Version < newestVersion {
			val.Version = newestVersion
		}
		val.Version += 1
		val.Timestamp = now
	}

	return val.Version, true
}

func SDFSUpdateFile(filename string, version int, deleteFlag bool, length int, content []byte) {
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
		log.Printf("[Verbose] Skip UpdateFile: file %v, version %v (latest version %v), delete %v",
			filename,
			version,
			val.Version,
			deleteFlag,
		)
		fmt.Printf("Skip UpdateFile: file %v, version %v (latest version %v), delete %v\n",
			filename,
			version,
			val.Version,
			deleteFlag,
		)
		return
	}
	val.Timestamp = time.Now()
	val.Version = version
	val.DeleteFlag = deleteFlag

	if !deleteFlag {
		WriteFile(SDFSPath(filename), content[0:length])
	} else {
		DeleteFile(SDFSPath(filename))
	}
	return
}

func SDFSReadFile(filename string) (int, bool, int, []byte, error) {
	SDFSFileInfoMapMux.RLock()
	val, ok := SDFSFileInfoMap[filename]
	SDFSFileInfoMapMux.RUnlock()

	if !ok {
		return -1, true, 0, nil, errors.New("file not found")
	}

	val.Lock.RLock()
	defer val.Lock.RUnlock()

	if val.DeleteFlag {
		return val.Version, true, 0, nil, nil
	}

	content, err := ReadFile(SDFSPath(filename))
	if err != nil {
		return -1, true, 0, nil, errors.New("read file error")
	}

	return val.Version, false, len(content), content, nil
}
