package common

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"
)

// TODO: add a queue to wait tasks
var (
	RpcAsyncCallerTaskWaitQueue    []*RpcAsyncCallerTask
	RpcAsyncCallerTaskWaitQueueMux sync.Mutex
)

type RpcClient struct {
}

type RpcS2S struct {
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

	filename := args.Filename
	deleteFlag := args.DeleteFlag

	k := SDFSHash(filename)
	mainReplica, replicaMap := GetReplicasByKey(k)
	if len(replicaMap) == 0 {
		reply.Flag = false
		reply.ErrStr = "no members in the cluster"
		return nil
	}

	// update file version # on main replica
	task := &RpcAsyncCallerTask{
		"UpdateFileVersion",
		mainReplica.Info,
		&ArgUpdateFileVersion{filename, args.ForceFlag},
		new(ReplyUpdateFileVersion),
		make(chan error),
	}
	go CallRpcS2SGeneral(task)
	err := <-task.Chan
	if err == nil && !task.Reply.(*ReplyUpdateFileVersion).Flag {
		err = errors.New(task.Reply.(*ReplyUpdateFileVersion).ErrStr)
	}
	if err != nil {
		log.Printf("[Error] Fail to update file version on main replica %v: %v",
			task.Info.Host,
			err,
		)
		reply.Flag = false
		reply.ErrStr = "fail to update file version on main replica " + task.Info.Host + ": " + err.Error()
		reply.NeedForce = task.Reply.(*ReplyUpdateFileVersion).NeedForce
		return nil
	}
	version := task.Reply.(*ReplyUpdateFileVersion).Version

	// prepare trunk argument
	argsTrunks := make([]*ArgUpdateFile, 0)
	offset := 0
	l := len(args.Content)
	for offset < l || deleteFlag {
		end := offset + SDFS_MAX_BUFFER_SIZE
		if end > l {
			end = l
		}
		argsTrunks = append(argsTrunks, &ArgUpdateFile{filename, deleteFlag, version, args.Length, offset, args.Content[offset:end]})

		if deleteFlag {
			break
		}

		offset = end
	}

	// send all PutFile trunk requests to all replicas
	cases := make([]reflect.SelectCase, len(replicaMap))
	hostIndexMap := make(map[int]HostInfo)
	i := 0
	for _, mem := range replicaMap {
		tmpChan := make(chan error)
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(tmpChan)}
		hostIndexMap[i] = mem.Info

		go func(host HostInfo, c chan error) {
			for _, argsTrunk := range argsTrunks {
				tk := &RpcAsyncCallerTask{
					"UpdateFile",
					host,
					argsTrunk,
					new(ReplyUpdateFile),
					make(chan error),
				}

				go CallRpcS2SGeneral(tk)

				err := <-tk.Chan
				if err == nil {
					log.Printf("[Info] Send UpdateFile to replica %v (file %v, version %v, delete %v, offset %v) success",
						host.Host,
						argsTrunk.Filename,
						argsTrunk.Version,
						argsTrunk.DeleteFlag,
						argsTrunk.Offset,
					)
					if tk.Reply.(*ReplyUpdateFile).Finish {
						log.Printf("[Info] Replica %v finishes to update file %v, version %v, delete %v",
							host.Host,
							argsTrunk.Filename,
							argsTrunk.Version,
							argsTrunk.DeleteFlag,
						)
					}
				} else {
					log.Printf("[Error] Fail to send UpdateFile to replica %v (file %v, version %v, delete %v, offset %v): %v",
						host.Host,
						argsTrunk.Filename,
						argsTrunk.Version,
						argsTrunk.DeleteFlag,
						argsTrunk.Offset,
						err,
					)
					c <- err
					return
				}
			}

			c <- nil
		}(mem.Info, tmpChan)
		i += 1
	}

	// wait for quorum finish updating
	i, j := 0, 0
	for i < SDFS_REPLICA_QUORUM_WRITE_SIZE && j < len(replicaMap) {
		chosen, value, _ := reflect.Select(cases)
		if hostInfo, ok := hostIndexMap[chosen]; ok {
			errInt := value.Interface()
			if errInt != nil {
				log.Printf("[Error] Quorum update %v-th node (host %v, file %v, version %v, delete %v) error: %v",
					j,
					hostInfo.Host,
					filename,
					version,
					deleteFlag,
					errInt.(error),
				)
			} else {
				log.Printf("[Info] Quorum update %v-th node (host %v, file %v, version %v, delete %v) success",
					j,
					hostInfo.Host,
					filename,
					version,
					deleteFlag,
				)
				i += 1
			}
			delete(hostIndexMap, chosen)
			j += 1
		}
	}
	if i < SDFS_REPLICA_QUORUM_WRITE_SIZE {
		reply.Flag = false
		reply.ErrStr = "fail to update file to enough replicas (quorum)"
	}

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

			args2 := ArgListFile(1)

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

//////////////////////////////////////////////
// RPC S2S
//////////////////////////////////////////////

func (t *RpcS2S) GrepFile(args *ArgGrep, reply *ReplyGrep) error {
	reply.Host = Cfg.Self.Host
	reply.Flag = true
	reply.ErrStr = ""
	reply.LineCount = 0
	reply.Files = []*GrepInfo{}
	for _, path := range args.Paths {
		info, err := GrepFile(path, args.RequestStr, args.IsRegex)
		if err == nil {
			reply.LineCount += len(info.Lines)
			reply.Files = append(reply.Files, info)
		}
	}

	return nil
}

func (t *RpcS2S) WriteFile(args *ArgWriteFile, reply *ReplyWriteFile) error {
	var err error = nil

	reply.Flag = true
	reply.ErrStr = ""
	reply.ByteWritten, err = WriteFile(args.Path, args.Content)
	if err != nil {
		reply.Flag = false
		reply.ErrStr = err.Error()
		reply.ByteWritten = 0
	}

	return nil
}

func (t *RpcS2S) MemberJoin(args *ArgMemberJoin, reply *ReplyMemberJoin) error {
	log.Printf("[Info] MemberJoin: host %v, port %v, udp_port %v, id %v",
		args.Host,
		args.Port,
		args.UdpPort,
		args.MachineID,
	)

	if Cfg.Self.Host == Cfg.Introducer.Host {
		// introducer should forward add member to all other members
		var tasks []*RpcAsyncCallerTask

		members := GetMemberList()
		for _, mem := range members {
			if mem.Info.Host == Cfg.Self.Host {
				continue
			}

			task := &RpcAsyncCallerTask{
				"MemberJoin",
				mem.Info,
				args,
				&ReplyMemberJoin{true, ""},
				make(chan error),
			}

			go CallRpcS2SGeneral(task)

			tasks = append(tasks, task)
		}

		// Wait for all RpcAsyncCallerTask
		for _, task := range tasks {
			err := <-task.Chan
			if err != nil {
				log.Printf("[Error] Fail to send MemberJoin to %v: %v",
					task.Info.Host,
					err,
				)
			}
		}
	}

	// ping the new node to add myself
	arg2 := ArgMemberAdd(Cfg.Self)

	task := RpcAsyncCallerTask{
		"MemberAdd",
		HostInfo{args.Host, args.Port, "", 0},
		&arg2,
		&ReplyMemberAdd{},
		make(chan error),
	}

	go CallRpcS2SGeneral(&task)

	err := <-task.Chan
	if err != nil {
		log.Printf("[Error] Fail to send MemberAdd to %v: %v",
			args.Host,
			err,
		)
	}

	inWindowFlag, newList, _ := AddMember(HostInfo(*args))
	if inWindowFlag {
		SDFSReplicaHostAdd(newList, HostInfo(*args))
	}
	return nil
}

func (t *RpcS2S) MemberAdd(args *ArgMemberAdd, reply *ReplyMemberAdd) error {
	log.Printf("[Info] MemberAdd: host %v, port %v, udp_port %v, id %v",
		args.Host,
		args.Port,
		args.UdpPort,
		args.MachineID,
	)

	AddMember(HostInfo(*args))
	return nil
}

func (t *RpcS2S) MemberLeave(args *ArgMemberLeave, reply *ReplyMemberLeave) error {
	log.Printf("[Info] MemberLeave: host %v, port %v, udp_port %v, id %v",
		args.Host,
		args.Port,
		args.UdpPort,
		args.MachineID,
	)

	inWindowFlag, oldList, _ := DeleteMember(HostInfo(*args))
	if inWindowFlag {
		SDFSReplicaHostDelete(oldList, HostInfo(*args))
	}
	return nil
}

func (t *RpcS2S) MemberFailure(args *ArgMemberFailure, reply *ReplyMemberFailure) error {
	// check if monitor is in the MemberList
	if !InMemberList(args.MonitorInfo) {
		return nil
	}

	if args.FailureInfo.Host != Cfg.Self.Host {
		inWindowFlag, oldList, _ := DeleteMember(args.FailureInfo)
		log.Printf("[Info] MemberFailure (detected by host %v): host %v, port %v, udp_port %v, id %v",
			args.MonitorInfo.Host,
			args.FailureInfo.Host,
			args.FailureInfo.Port,
			args.FailureInfo.UdpPort,
			args.FailureInfo.MachineID,
		)
		if inWindowFlag {
			SDFSReplicaHostDelete(oldList, args.FailureInfo)
		}
	} else {
		// if failure machine is myself
		// then it means false detection
		EraseMemberList()
		log.Printf("[Info] MemberFailure false detection (detected by host %v): host %v, port %v, udp_port %v, id %v",
			args.MonitorInfo.Host,
			args.FailureInfo.Host,
			args.FailureInfo.Port,
			args.FailureInfo.UdpPort,
			args.FailureInfo.MachineID,
		)
	}
	return nil
}

// MP3: file ops
func (t *RpcS2S) UpdateFileVersion(args *ArgUpdateFileVersion, reply *ReplyUpdateFileVersion) error {
	reply.Flag = true
	reply.ErrStr = ""
	reply.NeedForce = false
	reply.Version = -1

	version, ok := SDFSUpdateFileVersion(args.Filename, args.ForceFlag)
	if !ok {
		reply.Flag = false
		reply.ErrStr = "required force flag since last update time is too recent"
		reply.NeedForce = true
		log.Printf("[Warn] UpdateFileVersion: file %v abort, %v",
			args.Filename,
			reply.ErrStr,
		)
		log.Printf("UpdateFileVersion: file %v abort, %v\n",
			args.Filename,
			reply.ErrStr,
		)
	} else {
		reply.Version = version
		log.Printf("[Info] UpdateFileVersion: file %v, new version %v",
			args.Filename,
			version,
		)
		fmt.Printf("UpdateFileVersion: file %v, new version %v\n",
			args.Filename,
			version,
		)
	}
	return nil
}

func (t *RpcS2S) UpdateFile(args *ArgUpdateFile, reply *ReplyUpdateFile) error {
	reply.Flag = true
	reply.ErrStr = ""

	updated, finish := SDFSUpdateFile(args.Filename, args.Version, args.DeleteFlag, args.Length, args.Offset, args.Content)
	reply.Finish = finish

	log.Printf("[Info] UpdateFile (SKIP %v, FINISH %v): file %v, version %v, delete %v, file length %v, offset %v, trunk length %v",
		!updated,
		finish,
		args.Filename,
		args.Version,
		args.DeleteFlag,
		args.Length,
		args.Offset,
		len(args.Content),
	)
	fmt.Printf("UpdateFile (SKIP %v, FINISH %v): file %v, version %v, delete %v, file length %v, offset %v, trunk length %v\n",
		!updated,
		finish,
		args.Filename,
		args.Version,
		args.DeleteFlag,
		args.Length,
		args.Offset,
		len(args.Content),
	)

	return nil
}

func (t *RpcS2S) GetFile(args *ArgGetFile, reply *ReplyGetFile) error {
	reply.Flag = true
	reply.ErrStr = ""

	var err error
	reply.Version, reply.DeleteFlag, reply.Length, reply.Content, err = SDFSReadFile(args.Filename)
	if err == nil {
		log.Printf("[Info] GetFile: read file %v success (version %v, delete %v)",
			args.Filename,
			reply.Version,
			reply.DeleteFlag,
		)
		fmt.Printf("GetFile: read file %v success (version %v, delete %v)\n",
			args.Filename,
			reply.Version,
			reply.DeleteFlag,
		)
	} else {
		reply.Flag = false
		reply.ErrStr = err.Error()

		log.Printf("[Error] GetFile: read file %v error (%v)",
			args.Filename,
			err,
		)
		fmt.Printf("GetFile: read file %v error (%v)\n",
			args.Filename,
			err,
		)
	}

	return nil
}

func (t *RpcS2S) ExistFile(args *ArgExistFile, reply *ReplyExistFile) error {
	*reply = ReplyExistFile(SDFSExistFile(args.Filename))

	return nil
}

func (t *RpcS2S) ListFile(args *ArgListFile, reply *ReplyListFile) error {
	*reply = ReplyListFile(SDFSListFile())

	return nil
}
