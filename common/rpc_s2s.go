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

		tasks := make([]*RpcAsyncCallerTask, 0)
		masterInfo.Lock.Lock()
		if masterInfo.State == MASTER_STATE_WAIT_FOR_MAP_TASK || masterInfo.State == MASTER_STATE_WAIT_FOR_INTERMEDIATE_FILE {
			if _, ok := masterInfo.WorkerMap[args.FailureInfo.Host]; ok {
				N := len(masterInfo.WorkerList)
				for i, worker := range masterInfo.WorkerList {
					if args.FailureInfo.Host == worker.Info.Host {
						for j := i; j < N-1; j++ {
							masterInfo.WorkerList[j] = masterInfo.WorkerList[j+1]
						}
						masterInfo.WorkerList = masterInfo.WorkerList[:N-1]
						N -= 1
						break
					}
				}
				delete(masterInfo.WorkerMap, args.FailureInfo.Host)

				// send MapTaskDispatch
				idx := 0
				for _, filename := range masterInfo.DispatchFileMap[args.FailureInfo.Host] {
					target_worker := masterInfo.WorkerList[idx]
					task := &RpcAsyncCallerTask{
						"MapTaskDispatch",
						target_worker.Info,
						&ArgMapTaskDispatch{filename},
						new(ReplyMapTaskDispatch),
						make(chan error),
					}

					go CallRpcS2SGeneral(task)

					tasks = append(tasks, task)

					masterInfo.DispatchFileMap[target_worker.Info.Host] = append(masterInfo.DispatchFileMap[target_worker.Info.Host], filename)
					idx = (idx + 1) % N
				}

				delete(masterInfo.DispatchFileMap, args.FailureInfo.Host)
				delete(masterInfo.FinishFileCounter, args.FailureInfo.Host)
				delete(masterInfo.IntermediateDoneCounter, args.FailureInfo.Host)

				masterInfo.State = MASTER_STATE_WAIT_FOR_MAP_TASK
			}
		}
		masterInfo.Lock.Unlock()

		workerInfo.Lock.Lock()
		if _, ok := workerInfo.WorkerMap[args.FailureInfo.Host]; ok {
			N := len(workerInfo.WorkerList)
			nextWorker := (*MemberInfo)(nil)
			for i, worker := range workerInfo.WorkerList {
				if args.FailureInfo.Host == worker.Info.Host {
					for j := i; j < N-1; j++ {
						workerInfo.WorkerList[j] = workerInfo.WorkerList[j+1]
					}
					workerInfo.WorkerList = workerInfo.WorkerList[:N-1]
					N -= 1
					nextWorker = &workerInfo.WorkerList[i%N]
					break
				}
			}
			delete(workerInfo.WorkerMap, args.FailureInfo.Host)

			if nextWorker == nil {
				log.Printf("[Error][Map-worker] no next worker to send key-values after failure, this should not happen")
			} else {
				// send MapTaskSendKeyValues
				for filename, list := range workerInfo.KeyValueSent[args.FailureInfo.Host] {
					task := &RpcAsyncCallerTask{
						"MapTaskSendKeyValues",
						nextWorker.Info,
						&ArgMapTaskSendKeyValues{Cfg.Self, filename, list},
						new(ReplyMapTaskSendKeyValues),
						make(chan error),
					}

					go CallRpcS2SGeneral(task)

					tasks = append(tasks, task)

					if _, ok2 := workerInfo.KeyValueSent[nextWorker.Info.Host][filename]; !ok2 {
						workerInfo.KeyValueSent[nextWorker.Info.Host][filename] = list
					} else {
						workerInfo.KeyValueSent[nextWorker.Info.Host][filename] = append(workerInfo.KeyValueSent[nextWorker.Info.Host][filename], list...)
					}
				}
			}

			delete(workerInfo.KeyValueSent, args.FailureInfo.Host)
		}
		workerInfo.Lock.Unlock()

		// Wait for all RpcAsyncCallerTask
		for _, task := range tasks {
			<-task.Chan
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
		masterInfo.FinishFileCounter[mem.Info.Host] = 0
		masterInfo.IntermediateDoneCounter[mem.Info.Host] = 0
		masterInfo.WorkerList[cnt] = mem
		masterInfo.WorkerMap[mem.Info.Host] = mem
		cnt += 1
		if cnt >= workerNum {
			break
		}
	}
	masterInfo.IntermediateDoneNotifyToken = ""

	sendWorker := make([]MemberInfo, workerNum)
	copy(sendWorker, masterInfo.WorkerList)

	// send MapTaskPrepareWorker
	tasks = make([]*RpcAsyncCallerTask, 0)
	for _, worker := range masterInfo.WorkerList {
		task := &RpcAsyncCallerTask{
			"MapTaskPrepareWorker",
			worker.Info,
			&ArgMapTaskPrepareWorker{args.ExecFilename, Cfg.Self, sendWorker, workerNum, args.IntermediateFilenamePrefix},
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

	for {
		tasks = make([]*RpcAsyncCallerTask, 0)
		finishFlag := false

		masterInfo.Lock.Lock()
		if masterInfo.State == MASTER_STATE_WAIT_FOR_MAP_TASK {
			// check finish file counter
			flag := true
			for key, value := range masterInfo.FinishFileCounter {
				if value != len(masterInfo.DispatchFileMap[key]) {
					flag = false
					break
				}
			}
			if flag {
				masterInfo.State = MASTER_STATE_WAIT_FOR_INTERMEDIATE_FILE
				masterInfo.IntermediateDoneNotifyToken = GenRandomString(16)
				for key, _ := range masterInfo.IntermediateDoneCounter {
					masterInfo.IntermediateDoneCounter[key] = 0
				}

				// send write intermediate file command with new token and a worker list view
				workerCopy := make([]MemberInfo, len(masterInfo.WorkerList))
				copy(workerCopy, masterInfo.WorkerList)
				for _, worker := range masterInfo.WorkerList {
					masterInfo.IntermediateDoneCounter[worker.Info.Host] = 0
					task := &RpcAsyncCallerTask{
						"MapTaskWriteIntermediateFile",
						worker.Info,
						&ArgMapTaskWriteIntermediateFile{masterInfo.IntermediateDoneNotifyToken, workerCopy},
						new(ReplyMapTaskWriteIntermediateFile),
						make(chan error),
					}

					go CallRpcS2SGeneral(task)

					tasks = append(tasks, task)
				}
			}
		} else {
			// check intermediate done counter
			flag := true
			for _, value := range masterInfo.IntermediateDoneCounter {
				if value != 1 {
					flag = false
					break
				}
			}
			if flag {
				finishFlag = true
				masterInfo.State = MASTER_STATE_MAP_TASK_DONE

				// send MapTaskFinish
				argTmp := ArgMapTaskFinish(1)
				for _, worker := range masterInfo.WorkerList {
					task := &RpcAsyncCallerTask{
						"MapTaskFinish",
						worker.Info,
						&argTmp,
						new(ReplyMapTaskFinish),
						make(chan error),
					}

					go CallRpcS2SGeneral(task)

					tasks = append(tasks, task)
				}
			}
		}
		masterInfo.Lock.Unlock()

		// Wait for all RpcAsyncCallerTask
		for _, task := range tasks {
			<-task.Chan
		}

		if finishFlag {
			break
		}

		time.Sleep(1 * time.Second)
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
	workerInfo.IntermediateFilenamePrefix = args.IntermediateFilenamePrefix
	workerInfo.MasterHost = args.MasterHost
	workerInfo.InitWorkerNum = args.WorkerNum
	workerInfo.WorkerList = args.WorkerList
	workerInfo.WorkerMap = make(map[string]MemberInfo)
	workerInfo.KeyValueReceived = make(map[string]map[string][]MapReduceKeyValue)
	workerInfo.KeyValueSent = make(map[string]map[string][]MapReduceKeyValue)

	for _, worker := range workerInfo.WorkerList {
		workerInfo.WorkerMap[worker.Info.Host] = worker
		workerInfo.KeyValueSent[worker.Info.Host] = make(map[string][]MapReduceKeyValue)
	}

	workerInfo.WrittenKey = make(map[string]int)
	workerInfo.IntermediateFileWriteRequestToken = ""
	workerInfo.IntermediateFileWriteRequestView = nil

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

func (t *RpcS2S) MapTaskSendKeyValues(args *ArgMapTaskSendKeyValues, reply *ReplyMapTaskSendKeyValues) error {
	bucket := make(map[string][]MapReduceKeyValue)
	for _, obj := range args.Data {
		if _, ok := bucket[obj.Key]; !ok {
			bucket[obj.Key] = make([]MapReduceKeyValue, 0)
		}
		bucket[obj.Key] = append(bucket[obj.Key], obj)
	}

	workerInfo.Lock.Lock()
	if _, ok := workerInfo.WorkerMap[args.Sender.Host]; !ok {
		reply.Flag = false
		reply.ErrStr = "not in worker list"
		log.Printf("[Warn][Map-worker] worker %v is not in worker list", args.Sender.Host)
	} else {
		reply.Flag = true
		reply.ErrStr = ""

		for key, value_list := range bucket {
			if _, ok := workerInfo.KeyValueReceived[key]; !ok {
				workerInfo.KeyValueReceived[key] = make(map[string][]MapReduceKeyValue)
			}

			if _, ok := workerInfo.KeyValueReceived[key][args.InputFilename]; !ok {
				workerInfo.KeyValueReceived[key][args.InputFilename] = value_list
			}
		}
	}
	workerInfo.Lock.Unlock()

	return nil
}

func (t *RpcS2S) MapTaskWriteIntermediateFile(args *ArgMapTaskWriteIntermediateFile, reply *ReplyMapTaskWriteIntermediateFile) error {
	workerInfo.Lock.Lock()

	flag := false
	if workerInfo.IntermediateFileWriteRequestView == nil {
		flag = true
	}
	workerInfo.IntermediateFileWriteRequestToken = args.RequestToken
	workerInfo.IntermediateFileWriteRequestView = args.WorkerListView

	if flag {
		go MapTaskWriteIntermediateFiles()
	}

	workerInfo.Lock.Unlock()

	reply.Flag = true
	reply.ErrStr = ""

	return nil
}

func (t *RpcS2S) MapTaskNotifyMaster(args *ArgMapTaskNotifyMaster, reply *ReplyMapTaskNotifyMaster) error {
	reply.Flag = true
	reply.ErrStr = ""

	if args.Type == NOTIFY_TYPE_FINISH_MAP_TASK {
		masterInfo.Lock.Lock()
		if _, ok := masterInfo.FinishFileCounter[args.Sender.Host]; ok {
			masterInfo.FinishFileCounter[args.Sender.Host] += 1
		} else {
			log.Printf("[Warn][Map-master] ignore non-worker finish map task notification (%v)", args.Sender.Host)
		}
		masterInfo.Lock.Unlock()
	} else if args.Type == NOTIFY_TYPE_FINISH_INTERMEDIATE_FILE {
		masterInfo.Lock.Lock()
		if masterInfo.IntermediateDoneNotifyToken != args.Token {
			log.Printf("[Warn][Map-master] ignore out-of-date intermediate file notification (%v, %v)", args.Sender.Host, args.Token)
		} else {
			if _, ok := masterInfo.IntermediateDoneCounter[args.Sender.Host]; ok {
				masterInfo.IntermediateDoneCounter[args.Sender.Host] = 1
			} else {
				log.Printf("[Warn][Map-master] ignore non-worker intermediate file notification (%v, %v)", args.Sender.Host, args.Token)
			}
		}
		masterInfo.Lock.Unlock()
	} else {
		log.Printf("[Warn][Map-master] invalid notify type %v from %v", args.Type, args.Sender.Host)
		reply.Flag = false
		reply.ErrStr = "invalid type"
	}

	return nil
}

func (t *RpcS2S) MapTaskFinish(args *ArgMapTaskFinish, reply *ReplyMapTaskFinish) error {
	workerInfo.Lock.Lock()

	workerInfo.State = -1
	workerInfo.ExecFilePath = ""
	workerInfo.IntermediateFilenamePrefix = ""
	workerInfo.MasterHost = HostInfo{}
	workerInfo.InitWorkerNum = 0
	workerInfo.WorkerList = make([]MemberInfo, 0)
	workerInfo.WorkerMap = make(map[string]MemberInfo)
	workerInfo.KeyValueReceived = make(map[string]map[string][]MapReduceKeyValue)
	workerInfo.KeyValueSent = make(map[string]map[string][]MapReduceKeyValue)
	workerInfo.WrittenKey = make(map[string]int)
	workerInfo.IntermediateFileWriteRequestToken = ""
	workerInfo.IntermediateFileWriteRequestView = nil

	workerInfo.Lock.Unlock()

	reply.Flag = true
	reply.ErrStr = ""

	return nil
}
