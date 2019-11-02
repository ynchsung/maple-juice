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

type SDFSFileInfo2 struct {
	Filename   string
	Key        int
	Version    int
	DeleteFlag bool
}

type SDFSReplicaTransferTask struct {
	Target   HostInfo
	FileInfo *SDFSFileInfo
	Chan     chan error
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
		val = &SDFSVersionSequence{
			newestVersion + 100,
			now,
		}
		SDFSFileVersionSequenceMap[filename] = val
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

func SDFSExistFile(filename string) bool {
	SDFSFileInfoMapMux.RLock()
	val, ok := SDFSFileInfoMap[filename]
	SDFSFileInfoMapMux.RUnlock()

	if !ok {
		return false
	}

	val.Lock.RLock()
	defer val.Lock.RUnlock()

	return !val.DeleteFlag
}

func SDFSListFile() []SDFSFileInfo2 {
	SDFSFileInfoMapMux.RLock()
	defer SDFSFileInfoMapMux.RUnlock()

	ret := make([]SDFSFileInfo2, 0)
	for _, file := range SDFSFileInfoMap {
		file.Lock.RLock()
		ret = append(ret, SDFSFileInfo2{file.Filename, file.Key, file.Version, file.DeleteFlag})
		file.Lock.RUnlock()
	}

	return ret
}

func SDFSEraseFile() error {
	SDFSFileInfoMapMux.Lock()
	defer SDFSFileInfoMapMux.Unlock()

	SDFSFileInfoMap = make(map[string]*SDFSFileInfo)

	SDFSFileVersionSequenceMapMux.Lock()
	defer SDFSFileVersionSequenceMapMux.Unlock()

	SDFSFileVersionSequenceMap = make(map[string]*SDFSVersionSequence)

	return nil
}

func SDFSDoReplicaTransferTasks(tasks []*SDFSReplicaTransferTask) {
	for _, t := range tasks {
		go func(task *SDFSReplicaTransferTask) {
			var (
				content []byte = nil
				length  int    = 0
				err     error  = nil
			)

			task.FileInfo.Lock.RLock()
			if !task.FileInfo.DeleteFlag {
				content, err = ReadFile(SDFSPath(task.FileInfo.Filename))
				if err != nil {
					task.Chan <- err
					return
				}
				length = len(content)
			}

			rpcTask := &RpcAsyncCallerTask{
				"UpdateFile",
				task.Target,
				&ArgUpdateFile{task.FileInfo.Filename, task.FileInfo.DeleteFlag, task.FileInfo.Version, length, content},
				new(ReplyUpdateFile),
				make(chan error),
			}
			task.FileInfo.Lock.RUnlock()

			go CallRpcS2SGeneral(rpcTask)

			err = <-rpcTask.Chan
			task.Chan <- err
		}(t)
	}

	// Wait for all SDFSReplicaTransferTask
	for _, task := range tasks {
		err := <-task.Chan
		if err != nil {
			log.Printf("[Warn] Fail to send replica to %v: %v",
				task.Target.Host,
				err,
			)
		} else {
			log.Printf("[Info] Send replica to %v, file %v success",
				task.Target.Host,
				task.FileInfo.Filename,
			)
		}
	}
}

func SDFSReplicaHostAdd(memberList []MemberInfo, addHost HostInfo) error {
	var tasks []*SDFSReplicaTransferTask
	var removeList []string = make([]string, 0)
	N := len(memberList)

	SDFSFileInfoMapMux.Lock()
	SDFSFileVersionSequenceMapMux.Lock()
	for filename, val := range SDFSFileInfoMap {
		val.Lock.RLock()
		k := val.Key
		val.Lock.RUnlock()

		flag := false
		for i, mem := range memberList {
			if mem.Info.MachineID >= k {
				// main replica == i
				for j := 0; j < SDFS_REPLICA_NUM; j++ {
					if addHost.MachineID == memberList[(i+j)%N].Info.MachineID {
						task := &SDFSReplicaTransferTask{
							addHost,
							val,
							make(chan error),
						}
						tasks = append(tasks, task)
						break
					}
				}

				// remove file from map if it was the original last replica
				if memberList[(i+SDFS_REPLICA_NUM)%N].Info.MachineID == Cfg.Self.MachineID {
					removeList = append(removeList, filename)
				}

				flag = true
				break
			}
		}

		if !flag {
			// main replica == 0
			for j := 0; j < SDFS_REPLICA_NUM; j++ {
				if addHost.MachineID == memberList[j%N].Info.MachineID {
					task := &SDFSReplicaTransferTask{
						addHost,
						val,
						make(chan error),
					}
					tasks = append(tasks, task)
					break
				}
			}

			// remove file from map if it was the original last replica
			if memberList[SDFS_REPLICA_NUM%N].Info.MachineID == Cfg.Self.MachineID {
				removeList = append(removeList, filename)
			}
		}
	}

	for _, removeFilename := range removeList {
		delete(SDFSFileInfoMap, removeFilename)
		delete(SDFSFileVersionSequenceMap, removeFilename)
	}
	SDFSFileVersionSequenceMapMux.Unlock()
	SDFSFileInfoMapMux.Unlock()

	SDFSDoReplicaTransferTasks(tasks)

	return nil
}

func SDFSReplicaHostDelete(memberList []MemberInfo, deleteHost HostInfo) error {
	var tasks []*SDFSReplicaTransferTask
	N := len(memberList)

	SDFSFileInfoMapMux.RLock()
	for _, val := range SDFSFileInfoMap {
		val.Lock.RLock()
		k := val.Key
		val.Lock.RUnlock()

		flag := false
		for i, mem := range memberList {
			if mem.Info.MachineID >= k {
				// main replica == i
				for j := 0; j < SDFS_REPLICA_NUM; j++ {
					if deleteHost.MachineID == memberList[(i+j)%N].Info.MachineID {
						task := &SDFSReplicaTransferTask{
							memberList[(i+SDFS_REPLICA_NUM)%N].Info,
							val,
							make(chan error),
						}
						tasks = append(tasks, task)
						break
					}
				}
				flag = true
				break
			}
		}

		if !flag {
			// main replica == 0
			for j := 0; j < SDFS_REPLICA_NUM; j++ {
				if deleteHost.MachineID == memberList[j%N].Info.MachineID {
					task := &SDFSReplicaTransferTask{
						memberList[SDFS_REPLICA_NUM%N].Info,
						val,
						make(chan error),
					}
					tasks = append(tasks, task)
					break
				}
			}
		}
	}
	SDFSFileInfoMapMux.RUnlock()

	SDFSDoReplicaTransferTasks(tasks)

	return nil
}
