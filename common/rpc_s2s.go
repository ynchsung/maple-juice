package common

import (
	"fmt"
	"log"
	"path/filepath"
	"time"
)

type RpcS2S struct {
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
	masterInfo.Lock.RLock()
	tmp := masterInfo.State
	masterInfo.Lock.RUnlock()

	if tmp != MASTER_STATE_NONE {
		reply.Flag = false
		reply.ErrStr = "there is a map task still working"
		return nil
	}
	/*
	 * when receive this rpc
	 * it means I am master node
	 */
	members := GetMemberList()

	// TODO: check args.ExecFilename exists on the cluster

	/*
	 * =======================================================================
	 * step 0: get all input files with prefix args.InputFilenamePrefix
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
	 * step 1: init worker list and multicast to all workers
	 * =======================================================================
	 */
	masterInfo.Lock.Lock()

	// refresh memberlist in order to prevent race condition
	members = GetMemberList()
	workerNum := args.MachineNum - 1
	masterInfo.WorkerList = make([]MemberInfo, workerNum)
	cnt := 0
	for _, mem := range members {
		if mem.Info.Host == Cfg.Self.Host {
			continue
		}
		masterInfo.DispatchFileMap[mem.Info.Host] = make([]string, 0)
		masterInfo.Counter[mem.Info.Host] = 0
		masterInfo.WorkerList[cnt] = mem
		cnt += 1
		if cnt >= workerNum {
			break
		}
	}

	sendWorker := make([]MemberInfo, workerNum)
	copy(sendWorker, masterInfo.WorkerList)

	// send MapTaskPrepareWorker
	tasks = make([]*RpcAsyncCallerTask, 0)
	for _, worker := range masterInfo.WorkerList {
		task := &RpcAsyncCallerTask{
			"MapTaskPrepareWorker",
			worker.Info,
			&ArgMapTaskPrepareWorker{args.ExecFilename, Cfg.Self, sendWorker, workerNum},
			new(ReplyMapTaskPrepareWorker),
			make(chan error),
		}

		go CallRpcS2SGeneral(task)

		tasks = append(tasks, task)
	}
	masterInfo.State = MASTER_STATE_PREPARE
	masterInfo.Lock.Unlock()

	// Wait for all RpcAsyncCallerTask
	for _, task := range tasks {
		<-task.Chan
	}

	/*
	 * =======================================================================
	 * step 2: dispatch map task (input files) to all workers
	 * =======================================================================
	 */
	masterInfo.Lock.Lock()

	// round robin dispatch
	cnt = 0
	for filename, _ := range inputFiles {
		tmpHost := masterInfo.WorkerList[cnt].Info.Host
		masterInfo.DispatchFileMap[tmpHost] = append(masterInfo.DispatchFileMap[tmpHost], filename)
		cnt = (cnt + 1) % workerNum
	}

	// send MapTaskDispatch
	tasks = make([]*RpcAsyncCallerTask, 0)
	for _, worker := range masterInfo.WorkerList {
		for _, filename := range masterInfo.DispatchFileMap[worker.Info.Host] {
			task := &RpcAsyncCallerTask{
				"MapTaskDispatch",
				worker.Info,
				&ArgMapTaskDispatch{filename},
				new(ReplyMapTaskDispatch),
				make(chan error),
			}

			go CallRpcS2SGeneral(task)

			tasks = append(tasks, task)
		}
	}
	masterInfo.State = MASTER_STATE_WAIT_FOR_MAP_TASK
	masterInfo.Lock.Unlock()

	// Wait for all RpcAsyncCallerTask
	for _, task := range tasks {
		<-task.Chan
	}

	/*
	 * =======================================================================
	 * step 3: Wait for counter, i.e. wait for worker finish all map tasks
	 * =======================================================================
	 */
	for {
		masterInfo.Lock.Lock()
		flag := true
		for key, value := range masterInfo.Counter {
			if value != len(masterInfo.DispatchFileMap[key]) {
				flag = false
				break
			}
		}
		if flag {
			masterInfo.State = MASTER_STATE_WAIT_FOR_INTERMEDIATE_FILE
		}
		masterInfo.Lock.Unlock()

		if flag {
			break
		}
		time.Sleep(1 * time.Second)
	}

	/*
	 * =======================================================================
	 * step 4: send write intermediate file command
	 * =======================================================================
	 */
	masterInfo.Lock.Lock()
	tasks = make([]*RpcAsyncCallerTask, 0)
	for _, worker := range masterInfo.WorkerList {
		masterInfo.Counter[worker.Info.Host] = 0
		task := &RpcAsyncCallerTask{
			"MapTaskWriteIntermediateFile",
			worker.Info,
			&ArgMapTaskWriteIntermediateFile{},     // TODO
			new(ReplyMapTaskWriteIntermediateFile), // TODO
			make(chan error),
		}

		go CallRpcS2SGeneral(task)

		tasks = append(tasks, task)
	}
	masterInfo.State = MASTER_STATE_WAIT_FOR_INTERMEDIATE_FILE
	masterInfo.Lock.Unlock()

	// Wait for all RpcAsyncCallerTask
	for _, task := range tasks {
		<-task.Chan
	}

	/*
	 * =======================================================================
	 * step 5: Wait for counter
	 * i.e. wait for worker finish writing intermediate files
	 * =======================================================================
	 */
	for {
		masterInfo.Lock.Lock()
		flag := true
		for _, value := range masterInfo.Counter {
			if value != 1 {
				flag = false
				break
			}
		}
		if flag {
			masterInfo.State = MASTER_STATE_MAP_TASK_DONE

			// send MapTaskFinish
			tasks = make([]*RpcAsyncCallerTask, 0)
			argTmp := ArgMapTaskFinish(1)
			for _, worker := range masterInfo.WorkerList {
				task := &RpcAsyncCallerTask{
					"MapTaskFinish",
					worker.Info,
					&argTmp,                 // TODO
					new(ReplyMapTaskFinish), // TODO
					make(chan error),
				}

				go CallRpcS2SGeneral(task)

				tasks = append(tasks, task)
			}
		}
		masterInfo.Lock.Unlock()

		if flag {
			break
		}
		time.Sleep(1 * time.Second)
	}

	// Wait for all RpcAsyncCallerTask
	for _, task := range tasks {
		<-task.Chan
	}

	reply.Flag = true
	reply.ErrStr = ""
	return nil
}

func (t *RpcS2S) MapTaskPrepareWorker(args *ArgMapTaskPrepareWorker, reply *ReplyMapTaskPrepareWorker) error {
	// download exec file from SDFS
	content, length, err := SDFSDownloadFile(args.ExecFilename, Cfg.Self)
	if err != nil {
		reply.Flag = false
		reply.ErrStr = err.Error()
		log.Printf("[Error][Map-worker] cannot get executable file %v: %v", args.ExecFilename, err)
		return nil
	}

	storeStr := filepath.Join("./", Cfg.MapReduceWorkDir, SDFSGenerateRandomFilename(8))
	_, _ = WriteFile(storeStr, content[0:length])

	workerInfo.Lock.Lock()

	workerInfo.ExecFilePath = storeStr
	workerInfo.MasterHost = args.MasterHost
	workerInfo.WorkerNum = args.WorkerNum
	workerInfo.WorkerList = args.WorkerList

	workerInfo.KeyValueReceived = make(map[string][]MapReduceKeyValue)
	workerInfo.KeyValueSent = make(map[string][]MapReduceKeyValue)
	for _, worker := range workerInfo.WorkerList {
		workerInfo.KeyValueReceived[worker.Info.Host] = make([]MapReduceKeyValue, 0)
		workerInfo.KeyValueSent[worker.Info.Host] = make([]MapReduceKeyValue, 0)
	}

	workerInfo.Lock.Unlock()

	reply.Flag = true
	reply.ErrStr = ""
	return nil
}

func (t *RpcS2S) MapTaskDispatch(args *ArgMapTaskDispatch, reply *ReplyMapTaskDispatch) error {
	go MapTask(args.InputFilename)

	reply.Flag = true
	reply.ErrStr = ""

	return nil
}
