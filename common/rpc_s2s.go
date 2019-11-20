package common

import (
	"fmt"
	"log"
	"sync"
)

type RpcS2S struct {
}

type MapMasterInfo struct {
	IsMaster        bool
	DispatchFileMap map[string][]string
	Counter         map[string]int
	Lock            sync.RWMutex
}

var (
	masterInfo MapMasterInfo = MapMasterInfo{false, make(map[string][]string), make(map[string]int), sync.RWMutex{}}
)

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

	log.Printf("[Info] UpdateFile (SKIP %v, FINISH %v): file %v, version %v, delete %v, file length %v, offset %v, chunk length %v",
		!updated,
		finish,
		args.Filename,
		args.Version,
		args.DeleteFlag,
		args.Length,
		args.Offset,
		len(args.Content),
	)
	/*
		fmt.Printf("UpdateFile (SKIP %v, FINISH %v): file %v, version %v, delete %v, file length %v, offset %v, chunk length %v\n",
			!updated,
			finish,
			args.Filename,
			args.Version,
			args.DeleteFlag,
			args.Length,
			args.Offset,
			len(args.Content),
		)
	*/

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
	regex := string(*args)
	if len(regex) == 0 {
		*reply = ReplyListFile(SDFSListFile())
	} else {
		*reply = ReplyListFile(SDFSListFileByRegex(regex))
	}

	return nil
}

// MP4: Map Reduce
func (t *RpcS2S) MapTaskStart(args *ArgMapTaskStart, reply *ReplyMapTaskStart) error {
	/*
	 * when receive this rpc
	 * it means I am master node
	 */
	members := GetMemberList()

	// TODO: check args.ExecFilename exists on the cluster

	/*
	 * =======================================================================
	 * step 1: get all input files with prefix args.InputFilenamePrefix
	 * =======================================================================
	 */
	inputFiles := make(map[string]int)
	tasks := make([]*RpcAsyncCallerTask, 0)
	args2 := ArgListFile("^" + args.InputFilenamePrefix)
	for _, mem := range members {
		task := &RpcAsyncCallerTask{
			"ListFile",
			mem.Info,
			&args2,
			new(ReplyListFile),
			make(chan error),
		}

		go CallRpcS2SGeneral(task)

		tasks = append(tasks, task)
	}

	// Wait for all RpcAsyncCallerTask
	for _, task := range tasks {
		err := <-task.Chan
		// if error then skip
		if err == nil {
			for _, file := range []SDFSFileInfo2(*(task.Reply.(*ReplyListFile))) {
				inputFiles[file.Filename] = 1
			}
		}
	}

	/*
	 * =======================================================================
	 * step 2: dispatch map task (input files) to all workers
	 * =======================================================================
	 */
	masterInfo.Lock.Lock()
	masterInfo.IsMaster = true
	type MapMasterInfo struct {
		IsMaster        bool
		DispatchFileMap map[string][]string
		Counter         map[string]int
		Lock            sync.RWMutex
	}

	// refresh memberlist in order to prevent race condition
	members = GetMemberList()

	workerNum := args.MachineNum - 1
	workers := make([]MemberInfo, workerNum)
	cnt := 0
	for _, mem := range members {
		if mem.Info.Host == Cfg.Self.Host {
			continue
		}
		masterInfo.DispatchFileMap[mem.Info.Host] = make([]string, 0)
		masterInfo.Counter[mem.Info.Host] = 0
		workers[cnt] = mem
		cnt += 1
	}

	// round robin dispatch
	cnt = 0
	for filename, _ := range inputFiles {
		masterInfo.DispatchFileMap[workers[cnt].Info.Host] = append(masterInfo.DispatchFileMap[workers[cnt].Info.Host], filename)
		cnt = (cnt + 1) % workerNum
	}
	masterInfo.Lock.Unlock()

	tasks = make([]*RpcAsyncCallerTask, 0)
	for _, worker := range workers {
		for _, filename := range masterInfo.DispatchFileMap[worker.Info.Host] {
			task := &RpcAsyncCallerTask{
				"MapTaskDispatch",
				worker.Info,
				&ArgMapTaskDispatch{args.ExecFilename, filename, Cfg.Self},
				new(ReplyMapTaskDispatch),
				make(chan error),
			}

			go CallRpcS2SGeneral(task)

			tasks = append(tasks, task)
		}
	}

	// Wait for all RpcAsyncCallerTask
	for _, task := range tasks {
		<-task.Chan
	}

	/*
	 * =======================================================================
	 * step 3: Wait for counter, send write intermediate file command
	 * =======================================================================
	 */
	return nil
}
