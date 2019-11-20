package common

import (
	"errors"
	"log"
	"reflect"
	"sync"
	"time"
)

type UpdateFileRequest struct {
	Token      string
	ReplicaMap map[string]MemberInfo
	Version    int
	Flag       bool
	ErrStr     string
	NeedForce  bool

	FinishReplicaMap map[string]int
	Lock             sync.RWMutex
}

// TODO: add a queue to wait tasks
var (
	RpcAsyncCallerTaskWaitQueue    []*RpcAsyncCallerTask
	RpcAsyncCallerTaskWaitQueueMux sync.Mutex

	RpcUpdateFileRequestMap    map[string]*UpdateFileRequest = make(map[string]*UpdateFileRequest)
	RpcUpdateFileRequestMapMux sync.RWMutex
)

type RpcClient struct {
}

//////////////////////////////////////////////
// RPC Client
//////////////////////////////////////////////

func (t *RpcClient) GrepFile(args *ArgGrep, reply *ReplyGrepList) error {
	var tasks []*RpcAsyncCallerTask

	// Send RpcS2S Grep File for all machines in the cluster
	for _, host := range Cfg.ClusterInfo.Hosts {
		task := &RpcAsyncCallerTask{
			"GrepFile",
			host,
			args,
			&ReplyGrep{host.Host, true, "", 0, []*GrepInfo{}},
			make(chan error),
		}

		go CallRpcS2SGrepFile(host.Host, host.Port, args, task.Reply.(*ReplyGrep), task.Chan)

		tasks = append(tasks, task)
		*reply = append(*reply, task.Reply.(*ReplyGrep))
	}

	// Wait for all RpcAsyncCallerTask
	for _, task := range tasks {
		err := <-task.Chan
		if err != nil {
			log.Printf("[Error] Fail to send GrepFile to %v: %v",
				task.Info.Host,
				err,
			)
		}
	}

	return nil
}

func (t *RpcClient) GetMachineID(args *ArgGetMachineID, reply *ReplyGetMachineID) error {
	*reply = ReplyGetMachineID(Cfg.Self)
	return nil
}

func (t *RpcClient) GetMemberList(args *ArgGetMemberList, reply *ReplyGetMemberList) error {
	*reply = GetMemberList()
	return nil
}

func (t *RpcClient) MemberJoin(args *ArgClientMemberJoin, reply *ReplyClientMemberJoin) error {
	if InMemberList(Cfg.Self) {
		reply.Flag = false
		reply.ErrStr = "Already join"
		return nil
	}

	AddMember(Cfg.Self)
	if Cfg.Self.Host != Cfg.Introducer.Host {
		// not introducer
		args2 := ArgMemberJoin(Cfg.Self)

		task := RpcAsyncCallerTask{
			"MemberJoin",
			Cfg.Introducer,
			&args2,
			&ReplyMemberJoin{},
			make(chan error),
		}

		go CallRpcS2SGeneral(&task)

		err := <-task.Chan
		if err != nil {
			log.Printf("[Error] Join error: %v", err)
			reply.Flag = false
			reply.ErrStr = err.Error()
			return nil
		}
	}

	log.Printf("[Info] Join")
	reply.Flag = true
	reply.ErrStr = ""

	return nil
}

func (t *RpcClient) MemberLeave(args *ArgClientMemberLeave, reply *ReplyClientMemberLeave) error {
	members := GetMemberList()
	EraseMemberList()
	SDFSEraseFile()

	time.Sleep(time.Second)

	// Send RpcS2S Leave to all members
	var tasks []*RpcAsyncCallerTask
	args2 := ArgMemberLeave(Cfg.Self)
	for _, mem := range members {
		if mem.Info.Host == Cfg.Self.Host {
			continue
		}

		task := &RpcAsyncCallerTask{
			"MemberLeave",
			mem.Info,
			&args2,
			&ReplyMemberLeave{true, ""},
			make(chan error),
		}

		go CallRpcS2SGeneral(task)

		tasks = append(tasks, task)
	}

	// Wait for all RpcAsyncCallerTask
	for _, task := range tasks {
		err := <-task.Chan
		if err != nil {
			log.Printf("[Error] Fail to send MemberLeave to %v: %v",
				task.Info.Host,
				err,
			)
		}
	}

	log.Printf("[Info] Leave")
	reply.Flag = true
	reply.ErrStr = ""

	return nil
}

// MP3: file ops
func (t *RpcClient) UpdateFile(args *ArgClientUpdateFile, reply *ReplyClientUpdateFile) error {
	reply.Flag = true
	reply.ErrStr = ""
	reply.Finish = false
	reply.NeedForce = false

	requestToken := args.RequestToken

	if args.Offset == 0 {
		// if the request is the first chunk
		// then it should request a new version # from the main replica
		// and add the info (requestInfo) to RpcUpdateFileRequestMap
		// in order to let other chunk's requests thread get this info
		requestInfo := &UpdateFileRequest{
			requestToken,
			nil,
			0,
			true,
			"",
			false,
			make(map[string]int),
			sync.RWMutex{},
		}

		mainReplica, replicaMap := GetReplicasByKey(SDFSHash(args.Filename))
		if len(replicaMap) == 0 {
			requestInfo.Flag = false
			requestInfo.ErrStr = "no members in the cluster"
		} else {
			requestInfo.ReplicaMap = replicaMap

			// update file version # on main replica
			task := &RpcAsyncCallerTask{
				"UpdateFileVersion",
				mainReplica.Info,
				&ArgUpdateFileVersion{args.Filename, args.ForceFlag},
				new(ReplyUpdateFileVersion),
				make(chan error),
			}

			go CallRpcS2SGeneral(task)

			err := <-task.Chan
			if err == nil && !task.Reply.(*ReplyUpdateFileVersion).Flag {
				err = errors.New(task.Reply.(*ReplyUpdateFileVersion).ErrStr)
			}

			if err == nil {
				requestInfo.Version = task.Reply.(*ReplyUpdateFileVersion).Version
			} else {
				log.Printf("[Error] Fail to update file version on main replica %v: %v",
					task.Info.Host,
					err,
				)
				requestInfo.Flag = false
				requestInfo.ErrStr = "fail to update file version on main replica " + task.Info.Host + ": " + err.Error()
				requestInfo.NeedForce = task.Reply.(*ReplyUpdateFileVersion).NeedForce
			}
		}

		RpcUpdateFileRequestMapMux.Lock()
		RpcUpdateFileRequestMap[requestToken] = requestInfo
		RpcUpdateFileRequestMapMux.Unlock()
	}

	// since every chunk's update request needs the version info
	// we wait for requestInfo presenting in RpcUpdateFileRequestMap
	// i.e. wait for the first chunk getting the new version #
	requestInfo := (*UpdateFileRequest)(nil)
	for {
		RpcUpdateFileRequestMapMux.RLock()
		temp, ok := RpcUpdateFileRequestMap[requestToken]
		if ok {
			requestInfo = temp
		}
		RpcUpdateFileRequestMapMux.RUnlock()

		if requestInfo != nil {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	// if an error occurs
	// it means the main replica fails to give a new version # to the first chunk
	// abort update
	if !requestInfo.Flag {
		reply.Flag = requestInfo.Flag
		reply.ErrStr = requestInfo.ErrStr
		reply.Finish = false
		reply.NeedForce = requestInfo.NeedForce
		return nil
	}

	// forward chunk to replicas
	var tasks []*RpcAsyncCallerTask
	for _, mem := range requestInfo.ReplicaMap {
		task := &RpcAsyncCallerTask{
			"UpdateFile",
			mem.Info,
			&ArgUpdateFile{args.Filename, args.DeleteFlag, requestInfo.Version, args.Length, args.Offset, args.Content},
			new(ReplyUpdateFile),
			make(chan error),
		}

		go CallRpcS2SGeneral(task)

		tasks = append(tasks, task)
	}

	// Wait for all RpcAsyncCallerTask
	for _, task := range tasks {
		err := <-task.Chan
		if err != nil {
			log.Printf("[Error] send RpcUpdateFile to %v (file %v, version %v, delete %v, offset %v) error: %v",
				task.Info.Host,
				args.Filename,
				requestInfo.Version,
				args.DeleteFlag,
				args.Offset,
				err,
			)
		} else {
			if task.Reply.(*ReplyUpdateFile).Finish {
				requestInfo.Lock.Lock()
				requestInfo.FinishReplicaMap[task.Info.Host] = 1
				requestInfo.Lock.Unlock()
			}
		}
	}

	requestInfo.Lock.RLock()
	if len(requestInfo.FinishReplicaMap) >= SDFS_REPLICA_QUORUM_WRITE_SIZE {
		reply.Finish = true
	}
	requestInfo.Lock.RUnlock()

	return nil
}

func (t *RpcClient) GetFile(args *ArgClientGetFile, reply *ReplyClientGetFile) error {
	reply.Flag = true
	reply.ErrStr = ""

	filename := args.Filename

	k := SDFSHash(filename)
	_, replicaMap := GetReplicasByKey(k)
	if len(replicaMap) == 0 {
		reply.Flag = false
		reply.ErrStr = "no members in the cluster"
		return nil
	}

	// read from all replicas
	var tasks []*RpcAsyncCallerTask
	cases := make([]reflect.SelectCase, len(replicaMap))
	i := 0
	for _, mem := range replicaMap {
		task := &RpcAsyncCallerTask{
			"GetFile",
			mem.Info,
			&ArgGetFile{filename},
			new(ReplyGetFile),
			make(chan error),
		}

		go CallRpcS2SGeneral(task)

		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(task.Chan)}
		tasks = append(tasks, task)
		i += 1
	}

	// wait for quorum finish reading and choose the latest version
	maxVersion := -1
	i, j := 0, 0
	for i < SDFS_REPLICA_QUORUM_READ_SIZE && j < len(tasks) {
		chosen, value, _ := reflect.Select(cases)
		if tasks[chosen] != nil {
			errInt := value.Interface()
			if errInt != nil {
				log.Printf("[Error] Quorum read %v-th node (host %v, file %v) error: %v",
					j,
					tasks[chosen].Info.Host,
					filename,
					errInt.(error),
				)
			} else {
				r := tasks[chosen].Reply.(*ReplyGetFile)
				if !r.Flag {
					log.Printf("[Error] Quorum read %v-th node (host %v, file %v) error: %v",
						j,
						tasks[chosen].Info.Host,
						filename,
						r.ErrStr,
					)
				} else {
					log.Printf("[Info] Quorum read %v-th node (host %v, file %v, version %v, delete %v) success",
						j,
						tasks[chosen].Info.Host,
						filename,
						r.Version,
						r.DeleteFlag,
					)
					if r.Version > maxVersion {
						maxVersion = r.Version
						if r.DeleteFlag {
							reply.Flag = false
							reply.ErrStr = "file not found"
							reply.Length = 0
							reply.Content = nil
						} else {
							reply.Flag = true
							reply.ErrStr = ""
							reply.Length = r.Length
							reply.Content = r.Content
						}
					}
					i += 1
				}
			}
			tasks[chosen] = nil
			j += 1
		}
	}
	if i < SDFS_REPLICA_QUORUM_READ_SIZE {
		reply.Flag = false
		reply.ErrStr = "fail to read file from enough replicas (quorum)"
	} else if maxVersion < 0 {
		reply.Flag = false
		reply.ErrStr = "file not found"
		reply.Length = 0
		reply.Content = nil
	}

	// put not finishing task into queue to wait
	for _, task := range tasks {
		if task != nil {
			RpcAsyncCallerTaskWaitQueueMux.Lock()
			RpcAsyncCallerTaskWaitQueue = append(RpcAsyncCallerTaskWaitQueue, task)
			RpcAsyncCallerTaskWaitQueueMux.Unlock()
		}
	}
	return nil
}

func (t *RpcClient) ListHostsByFile(args *ArgClientListHostsByFile, reply *ReplyClientListHostsByFile) error {
	members := GetMemberList()

	// Send RpcS2S ExistFile to all members
	var tasks []*RpcAsyncCallerTask
	args2 := ArgExistFile{args.Filename}
	for _, mem := range members {
		task := &RpcAsyncCallerTask{
			"ExistFile",
			mem.Info,
			&args2,
			new(ReplyExistFile),
			make(chan error),
		}

		go CallRpcS2SGeneral(task)

		tasks = append(tasks, task)
	}

	reply.Flag = true
	reply.ErrStr = ""
	reply.Hosts = make([]HostInfo, 0)

	// Wait for all RpcAsyncCallerTask
	for _, task := range tasks {
		err := <-task.Chan
		if err != nil {
			log.Printf("[Error] Fail to send ExistFile to %v: %v",
				task.Info.Host,
				err,
			)
		} else {
			exist := bool(*(task.Reply.(*ReplyExistFile)))
			if exist {
				reply.Hosts = append(reply.Hosts, task.Info)
			}
		}
	}

	return nil
}

func (t *RpcClient) ListFilesByHost(args *ArgClientListFilesByHost, reply *ReplyClientListFilesByHost) error {
	members := GetMemberList()
	for _, mem := range members {
		if mem.Info.MachineID == args.MachineID {
			reply.Flag = true
			reply.ErrStr = ""

			args2 := ArgListFile(args.Regex)

			task := &RpcAsyncCallerTask{
				"ListFile",
				mem.Info,
				&args2,
				new(ReplyListFile),
				make(chan error),
			}

			go CallRpcS2SGeneral(task)

			err := <-task.Chan
			if err == nil {
				reply.Files = []SDFSFileInfo2(*(task.Reply.(*ReplyListFile)))
			} else {
				reply.Flag = false
				reply.ErrStr = err.Error()
			}

			return nil
		}
	}

	reply.Flag = false
	reply.ErrStr = "machine not in member list"

	return nil
}

// MP4: Map Reduce
func (t *RpcClient) MapTaskStart(args *ArgMapTaskStart, reply *ReplyMapTaskStart) error {
	master, N := GetMapReduceMaster()
	if N < args.MachineNum {
		reply.Flag = false
		reply.ErrStr = "Not enough node in cluster"
		return nil
	}

	task := &RpcAsyncCallerTask{
		"MapTaskStart",
		master.Info,
		args,
		reply,
		make(chan error),
	}

	go CallRpcS2SGeneral(task)

	err := <-task.Chan
	if err != nil {
		log.Printf("[Error] fail to start map task on master node (%v): %v", master.Info.Host, err)
	}

	return nil
}
