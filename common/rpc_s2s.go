package common

import (
	"fmt"
	"log"
	"path/filepath"
	"sort"
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
			close(task.Chan)
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
	close(task.Chan)
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

		// check if the master is working
		if masterInfo.State != MASTER_STATE_NONE && masterInfo.State != MASTER_STATE_MAP_TASK_DONE && masterInfo.State != MASTER_STATE_REDUCE_TASK_DONE {
			// check if the failing node is a worker
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

				if masterInfo.State == MASTER_STATE_MAP_WAIT_FOR_TASK || masterInfo.State == MASTER_STATE_MAP_WAIT_FOR_INTERMEDIATE_FILE {
					// map task: need to re-dispatch input files and switch back to MASTER_STATE_MAP_WAIT_FOR_TASK
					masterInfo.State = MASTER_STATE_MAP_WAIT_FOR_TASK

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
				} else if masterInfo.State == MASTER_STATE_REDUCE_WAIT_FOR_TASK {
					// reduce task: need to re-dispatch intermediate files

					// send ReduceTaskDispatch
					idx := 0
					for _, filename := range masterInfo.DispatchFileMap2[args.FailureInfo.Host] {
						target_worker := masterInfo.WorkerList[idx]
						task := &RpcAsyncCallerTask{
							"ReduceTaskDispatch",
							target_worker.Info,
							&ArgReduceTaskDispatch{filename},
							new(ReplyReduceTaskDispatch),
							make(chan error),
						}

						go CallRpcS2SGeneral(task)

						tasks = append(tasks, task)

						masterInfo.DispatchFileMap2[target_worker.Info.Host] = append(masterInfo.DispatchFileMap2[target_worker.Info.Host], filename)
						idx = (idx + 1) % N
					}

					delete(masterInfo.DispatchFileMap2, args.FailureInfo.Host)
					delete(masterInfo.ResultFileMap, args.FailureInfo.Host)
				}
			}
		}
		masterInfo.Lock.Unlock()

		workerInfo.Lock.Lock()
		if _, ok := workerInfo.WorkerMap[args.FailureInfo.Host]; ok {
			N := len(workerInfo.WorkerList)
			// nextWorker := (*WorkerInfo)(nil)
			for i, worker := range workerInfo.WorkerList {
				if args.FailureInfo.Host == worker.Info.Host {
					for j := i; j < N-1; j++ {
						workerInfo.WorkerList[j] = workerInfo.WorkerList[j+1]
					}
					workerInfo.WorkerList = workerInfo.WorkerList[:N-1]
					N -= 1
					// nextWorker = &workerInfo.WorkerList[i%N]
					break
				}
			}
			delete(workerInfo.WorkerMap, args.FailureInfo.Host)

			/*
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
			*/

			// delete(workerInfo.KeyValueSent, args.FailureInfo.Host)
		}
		workerInfo.Lock.Unlock()

		// Wait for all RpcAsyncCallerTask
		for _, task := range tasks {
			<-task.Chan
			close(task.Chan)
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
		/*
			fmt.Printf("UpdateFileVersion: file %v, new version %v\n",
				args.Filename,
				version,
			)
		*/
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
		/*
			fmt.Printf("GetFile: read file %v success (version %v, delete %v)\n",
				args.Filename,
				reply.Version,
				reply.DeleteFlag,
			)
		*/
	} else {
		reply.Flag = false
		reply.ErrStr = err.Error()

		log.Printf("[Error] GetFile: read file %v error (%v)",
			args.Filename,
			err,
		)
		/*
			fmt.Printf("GetFile: read file %v error (%v)\n",
				args.Filename,
				err,
			)
		*/
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

	// TODO: check args.ExecFilename exists on the cluster

	masterInfo.Lock.Lock()

	if masterInfo.State != MASTER_STATE_NONE && masterInfo.State != MASTER_STATE_REDUCE_TASK_DONE {
		reply.Flag = false
		reply.ErrStr = "there is a map reduce job still working"

		masterInfo.Lock.Unlock()

		return nil
	}

	/*
	 * =======================================================================
	 * step 0: init worker list and prepare all workers
	 * =======================================================================
	 */
	members := GetMemberList()
	workerNum := args.MachineNum - 1
	masterInfo.WorkerList = make([]WorkerInfo, workerNum)
	masterInfo.WorkerMap = make(map[string]WorkerInfo)
	masterInfo.DispatchFileMap = make(map[string][]string)
	masterInfo.FinishFileCounter = make(map[string]int)
	masterInfo.IntermediateDoneNotifyToken = ""
	masterInfo.IntermediateDoneCounter = make(map[string]int)
	cnt := 0
	for _, mem := range members {
		if mem.Info.Host == Cfg.Self.Host {
			continue
		}
		masterInfo.WorkerList[cnt] = WorkerInfo{mem.Info, cnt}
		masterInfo.WorkerMap[mem.Info.Host] = WorkerInfo{mem.Info, cnt}
		masterInfo.DispatchFileMap[mem.Info.Host] = make([]string, 0)
		masterInfo.FinishFileCounter[mem.Info.Host] = 0
		masterInfo.IntermediateDoneCounter[mem.Info.Host] = 0
		cnt += 1
		if cnt >= workerNum {
			break
		}
	}

	// send MapTaskPrepareWorker
	tasks := make([]*RpcAsyncCallerTask, 0)
	wl := make([]WorkerInfo, workerNum)
	copy(wl, masterInfo.WorkerList)
	for _, worker := range masterInfo.WorkerList {
		task := &RpcAsyncCallerTask{
			"MapTaskPrepareWorker",
			worker.Info,
			&ArgMapTaskPrepareWorker{args.ExecFilename, Cfg.Self, wl, workerNum, args.IntermediateFilenamePrefix},
			new(ReplyMapTaskPrepareWorker),
			make(chan error),
		}

		go CallRpcS2SGeneral(task)

		tasks = append(tasks, task)
	}

	masterInfo.State = MASTER_STATE_MAP_PREPARE

	masterInfo.Lock.Unlock()

	// Wait for all RpcAsyncCallerTask
	for _, task := range tasks {
		<-task.Chan
		close(task.Chan)
	}

	/*
	 * =======================================================================
	 * step 1: get all input files with prefix args.InputFilenamePrefix
	 * =======================================================================
	 */

	// refresh member list
	members = GetMemberList()

	tasks = make([]*RpcAsyncCallerTask, 0)
	inputFiles := make(map[string]int)
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
		close(task.Chan)
		// skip error
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
	masterInfo.State = MASTER_STATE_MAP_WAIT_FOR_TASK
	masterInfo.Lock.Unlock()

	// Wait for all RpcAsyncCallerTask
	for _, task := range tasks {
		<-task.Chan
		close(task.Chan)
	}

	xx := -1
	for {
		xx += 1
		tasks = make([]*RpcAsyncCallerTask, 0)
		finishFlag := false

		masterInfo.Lock.Lock()
		if masterInfo.State == MASTER_STATE_MAP_WAIT_FOR_TASK {
			// check finish file counter
			flag := true
			for key, value := range masterInfo.FinishFileCounter {
				if value != len(masterInfo.DispatchFileMap[key]) {
					flag = false
				}
				if xx%30 == 0 {
					fmt.Printf("<%v: %v/%v>", key, value, len(masterInfo.DispatchFileMap[key]))
				}
			}
			if xx%30 == 0 {
				fmt.Printf("\n")
			}
			if flag {
				masterInfo.State = MASTER_STATE_MAP_WAIT_FOR_INTERMEDIATE_FILE
				masterInfo.IntermediateDoneNotifyToken = GenRandomString(16)
				for key, _ := range masterInfo.IntermediateDoneCounter {
					masterInfo.IntermediateDoneCounter[key] = 0
				}

				// send write intermediate file command with new token and a worker list view
				wl := make([]WorkerInfo, len(masterInfo.WorkerList))
				copy(wl, masterInfo.WorkerList)
				for _, worker := range masterInfo.WorkerList {
					masterInfo.IntermediateDoneCounter[worker.Info.Host] = 0
					task := &RpcAsyncCallerTask{
						"MapTaskWriteIntermediateFile",
						worker.Info,
						&ArgMapTaskWriteIntermediateFile{masterInfo.IntermediateDoneNotifyToken, wl},
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
			for key, value := range masterInfo.IntermediateDoneCounter {
				if value != 1 {
					flag = false
					if xx%30 == 0 {
						fmt.Printf("<%v: %v/%v>", key, value, 1)
					}
				}
			}
			if xx%30 == 0 {
				fmt.Printf("\n")
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
			close(task.Chan)
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
	WriteFile(storeStr, content[0:length])

	workerInfo.Lock.Lock()

	workerInfo.ExecFilePath = storeStr
	workerInfo.IntermediateFilenamePrefix = args.IntermediateFilenamePrefix
	workerInfo.MasterHost = args.MasterHost
	workerInfo.InitWorkerNum = args.WorkerNum
	workerInfo.WorkerList = args.WorkerList
	workerInfo.WorkerMap = make(map[string]WorkerInfo)
	// workerInfo.KeyValueReceived = make(map[string]map[string][]MapReduceKeyValue)
	// workerInfo.KeyValueSent = make(map[string]map[string][]MapReduceKeyValue)

	for _, worker := range workerInfo.WorkerList {
		workerInfo.WorkerMap[worker.Info.Host] = worker
		// workerInfo.KeyValueSent[worker.Info.Host] = make(map[string][]MapReduceKeyValue)
	}

	workerInfo.KeyValueBucket = make([]map[string][]MapReduceKeyValue, args.WorkerNum)
	for i := 0; i < args.WorkerNum; i++ {
		workerInfo.KeyValueBucket[i] = make(map[string][]MapReduceKeyValue)
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
	MapReduceTaskQueueMux.Lock()
	MapReduceTaskQueue = append(MapReduceTaskQueue, &MapReduceTaskInfo{args.InputFilename, "map"})
	MapReduceTaskQueueMux.Unlock()

	reply.Flag = true
	reply.ErrStr = ""

	return nil
}

/*
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
*/

func (t *RpcS2S) MapTaskGetKeyValues(args *ArgMapTaskGetKeyValues, reply *ReplyMapTaskGetKeyValues) error {
	reply.Flag = true
	reply.ErrStr = ""
	reply.Data = make(map[string][]MapReduceKeyValue)

	workerInfo.Lock.Lock()

	for _, idx := range args.GetBucketIdxList {
		for filename, list := range workerInfo.KeyValueBucket[idx] {
			if _, ok := reply.Data[filename]; !ok {
				reply.Data[filename] = make([]MapReduceKeyValue, 0)
			}
			reply.Data[filename] = append(reply.Data[filename], list...)
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
	} else if args.Type == NOTIFY_TYPE_FINISH_MAP_INTERMEDIATE_FILE {
		masterInfo.Lock.Lock()
		if masterInfo.IntermediateDoneNotifyToken != args.Token {
			log.Printf("[Warn][Map-master] ignore out-of-date intermediate file notification (%v, %v)", args.Sender.Host, args.Token)
		} else {
			if !args.Flag {
				log.Printf("[Warn][Map-master] %v failed to write intermediate file (%v)", args.Sender.Host, args.Token)
			} else {
				if _, ok := masterInfo.IntermediateDoneCounter[args.Sender.Host]; ok {
					masterInfo.IntermediateDoneCounter[args.Sender.Host] = 1
					log.Printf("[Info][Map-master] %v succeed to write intermediate file (%v)", args.Sender.Host, args.Token)
				} else {
					log.Printf("[Warn][Map-master] ignore non-worker intermediate file notification (%v, %v)", args.Sender.Host, args.Token)
				}
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
	workerInfo.WorkerList = make([]WorkerInfo, 0)
	workerInfo.WorkerMap = make(map[string]WorkerInfo)
	// workerInfo.KeyValueReceived = make(map[string]map[string][]MapReduceKeyValue)
	// workerInfo.KeyValueSent = make(map[string]map[string][]MapReduceKeyValue)
	workerInfo.KeyValueBucket = make([]map[string][]MapReduceKeyValue, 0)
	workerInfo.WrittenKey = make(map[string]int)
	workerInfo.IntermediateFileWriteRequestToken = ""
	workerInfo.IntermediateFileWriteRequestView = nil

	workerInfo.Lock.Unlock()

	reply.Flag = true
	reply.ErrStr = ""

	return nil
}

func (t *RpcS2S) ReduceTaskStart(args *ArgReduceTaskStart, reply *ReplyReduceTaskStart) error {
	/*
	 * when receive this rpc
	 * it means I am master node
	 */

	// TODO: check args.ExecFilename exists on the cluster

	masterInfo.Lock.Lock()

	if masterInfo.State != MASTER_STATE_NONE && masterInfo.State != MASTER_STATE_MAP_TASK_DONE && masterInfo.State != MASTER_STATE_REDUCE_TASK_DONE {
		reply.Flag = false
		reply.ErrStr = "there is a map reduce job still working"

		masterInfo.Lock.Unlock()

		return nil
	}

	/*
	 * =======================================================================
	 * step 0: init worker list and prepare all workers
	 * =======================================================================
	 */
	members := GetMemberList()
	workerNum := args.MachineNum - 1
	masterInfo.WorkerList = make([]WorkerInfo, workerNum)
	masterInfo.WorkerMap = make(map[string]WorkerInfo)
	masterInfo.DispatchFileMap2 = make(map[string][]string)
	masterInfo.ResultFileMap = make(map[string]map[string][]MapReduceKeyValue)
	cnt := 0
	for _, mem := range members {
		if mem.Info.Host == Cfg.Self.Host {
			continue
		}
		masterInfo.WorkerList[cnt] = WorkerInfo{mem.Info, cnt}
		masterInfo.WorkerMap[mem.Info.Host] = WorkerInfo{mem.Info, cnt}
		masterInfo.DispatchFileMap2[mem.Info.Host] = make([]string, 0)
		masterInfo.ResultFileMap[mem.Info.Host] = make(map[string][]MapReduceKeyValue)
		cnt += 1
		if cnt >= workerNum {
			break
		}
	}

	// send ReduceTaskPrepareWorker
	tasks := make([]*RpcAsyncCallerTask, 0)
	wl := make([]WorkerInfo, workerNum)
	copy(wl, masterInfo.WorkerList)
	for _, worker := range masterInfo.WorkerList {
		task := &RpcAsyncCallerTask{
			"ReduceTaskPrepareWorker",
			worker.Info,
			&ArgReduceTaskPrepareWorker{args.ExecFilename, Cfg.Self, wl, workerNum},
			new(ReplyReduceTaskPrepareWorker),
			make(chan error),
		}

		go CallRpcS2SGeneral(task)

		tasks = append(tasks, task)
	}

	masterInfo.State = MASTER_STATE_REDUCE_PREPARE

	masterInfo.Lock.Unlock()

	// Wait for all RpcAsyncCallerTask
	for _, task := range tasks {
		<-task.Chan
		close(task.Chan)
	}

	/*
	 * =======================================================================
	 * step 1: get all input files with prefix args.IntermediateFilenamePrefix
	 * =======================================================================
	 */

	// refresh member list
	members = GetMemberList()

	tasks = make([]*RpcAsyncCallerTask, 0)
	files := make(map[string]int)
	args2 := ArgListFile("^" + args.IntermediateFilenamePrefix)
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
		close(task.Chan)
		// skip error
		if err == nil {
			for _, file := range []SDFSFileInfo2(*(task.Reply.(*ReplyListFile))) {
				files[file.Filename] = 1
			}
		}
	}

	/*
	 * =======================================================================
	 * step 2: dispatch reduce task (intermediate files) to all workers
	 * =======================================================================
	 */
	masterInfo.Lock.Lock()

	// round robin dispatch
	cnt = 0
	for filename, _ := range files {
		tmpHost := masterInfo.WorkerList[cnt].Info.Host
		masterInfo.DispatchFileMap2[tmpHost] = append(masterInfo.DispatchFileMap2[tmpHost], filename)
		cnt = (cnt + 1) % workerNum
	}

	// send ReduceTaskDispatch
	tasks = make([]*RpcAsyncCallerTask, 0)
	for _, worker := range masterInfo.WorkerList {
		for _, filename := range masterInfo.DispatchFileMap2[worker.Info.Host] {
			task := &RpcAsyncCallerTask{
				"ReduceTaskDispatch",
				worker.Info,
				&ArgReduceTaskDispatch{filename},
				new(ReplyReduceTaskDispatch),
				make(chan error),
			}

			go CallRpcS2SGeneral(task)

			tasks = append(tasks, task)
		}
	}
	masterInfo.State = MASTER_STATE_REDUCE_WAIT_FOR_TASK
	masterInfo.Lock.Unlock()

	// Wait for all RpcAsyncCallerTask
	for _, task := range tasks {
		<-task.Chan
		close(task.Chan)
	}

	for {
		tasks = make([]*RpcAsyncCallerTask, 0)

		masterInfo.Lock.Lock()

		// check result
		flag := true
		for key, _ := range masterInfo.ResultFileMap {
			if len(masterInfo.ResultFileMap[key]) != len(masterInfo.DispatchFileMap2[key]) {
				flag = false
				break
			}
		}
		if flag {
			masterInfo.State = MASTER_STATE_REDUCE_TASK_DONE

			// send ReduceTaskFinish
			argTmp := ArgReduceTaskFinish(1)
			for _, worker := range masterInfo.WorkerList {
				task := &RpcAsyncCallerTask{
					"ReduceTaskFinish",
					worker.Info,
					&argTmp,
					new(ReplyReduceTaskFinish),
					make(chan error),
				}

				go CallRpcS2SGeneral(task)

				tasks = append(tasks, task)
			}
		}

		masterInfo.Lock.Unlock()

		// Wait for all RpcAsyncCallerTask
		for _, task := range tasks {
			<-task.Chan
			close(task.Chan)
		}

		if flag {
			break
		}

		time.Sleep(1 * time.Second)
	}

	// put all results into args.OutputFilename
	results := make([]MapReduceKeyValue, 0)
	for _, mp := range masterInfo.ResultFileMap {
		for _, arr := range mp {
			results = append(results, arr...)
		}
	}
	sort.SliceStable(results, func(i, j int) bool {
		return results[i].Key < results[j].Key
	})

	content := ""
	for _, obj := range results {
		content = content + obj.Key + "\t" + obj.Value + "\n"
	}

	finish, _, err := SDFSUploadFile(Cfg.Self, args.OutputFilename, []byte(content), true)
	if err != nil {
		log.Printf("[Error][Reduce-master] cannot write output file %v: %v", args.OutputFilename, err)
		reply.Flag = false
		reply.ErrStr = err.Error()
	} else if !finish {
		log.Printf("[Error][Reduce-master] write output file didn't finish, this should not happen")
		reply.Flag = false
		reply.ErrStr = "write output file didn't finish, this should not happen"
	} else {
		log.Printf("[Info][Reduce-master] write output file %v success", args.OutputFilename)
		reply.Flag = true
		reply.ErrStr = ""

		if args.DeleteInput == 1 {
			chans := make([]chan error, 0)
			for filename, _ := range files {
				ch := make(chan error)

				go func(fn string, c chan error) {
					_, _, err := SDFSDeleteFile(Cfg.Self, fn, true)
					c <- err
				}(filename, ch)

				chans = append(chans, ch)
			}

			for _, ch := range chans {
				<-ch
				close(ch)
			}
		}
	}

	return nil
}

func (t *RpcS2S) ReduceTaskPrepareWorker(args *ArgReduceTaskPrepareWorker, reply *ReplyReduceTaskPrepareWorker) error {
	// download exec file from SDFS
	content, length, err := SDFSDownloadFile(args.ExecFilename, Cfg.Self)
	if err != nil {
		reply.Flag = false
		reply.ErrStr = err.Error()
		log.Printf("[Error][Reduce-worker] cannot get executable file %v: %v", args.ExecFilename, err)
		return nil
	}

	storeStr := filepath.Join("./", Cfg.MapReduceWorkDir, SDFSGenerateRandomFilename(8))
	WriteFile(storeStr, content[0:length])

	workerInfo.Lock.Lock()

	workerInfo.ExecFilePath = storeStr
	workerInfo.MasterHost = args.MasterHost
	workerInfo.InitWorkerNum = args.WorkerNum
	workerInfo.WorkerList = args.WorkerList
	workerInfo.WorkerMap = make(map[string]WorkerInfo)

	for _, worker := range workerInfo.WorkerList {
		workerInfo.WorkerMap[worker.Info.Host] = worker
	}

	workerInfo.Lock.Unlock()

	reply.Flag = true
	reply.ErrStr = ""
	return nil
}

func (t *RpcS2S) ReduceTaskDispatch(args *ArgReduceTaskDispatch, reply *ReplyReduceTaskDispatch) error {
	MapReduceTaskQueueMux.Lock()
	MapReduceTaskQueue = append(MapReduceTaskQueue, &MapReduceTaskInfo{args.IntermediateFilename, "reduce"})
	MapReduceTaskQueueMux.Unlock()

	reply.Flag = true
	reply.ErrStr = ""

	return nil
}

func (t *RpcS2S) ReduceTaskNotifyMaster(args *ArgReduceTaskNotifyMaster, reply *ReplyReduceTaskNotifyMaster) error {
	reply.Flag = true
	reply.ErrStr = ""

	masterInfo.Lock.Lock()
	if _, ok := masterInfo.ResultFileMap[args.Sender.Host]; ok {
		masterInfo.ResultFileMap[args.Sender.Host][args.Filename] = args.Data
	} else {
		log.Printf("[Warn][Reduce-master] ignore non-worker finish reduce task notification (%v)", args.Sender.Host)
	}
	masterInfo.Lock.Unlock()

	return nil
}

func (t *RpcS2S) ReduceTaskFinish(args *ArgReduceTaskFinish, reply *ReplyReduceTaskFinish) error {
	workerInfo.Lock.Lock()

	workerInfo.State = -1
	workerInfo.ExecFilePath = ""
	workerInfo.IntermediateFilenamePrefix = ""
	workerInfo.MasterHost = HostInfo{}
	workerInfo.InitWorkerNum = 0
	workerInfo.WorkerList = make([]WorkerInfo, 0)
	workerInfo.WorkerMap = make(map[string]WorkerInfo)
	workerInfo.KeyValueBucket = make([]map[string][]MapReduceKeyValue, 0)
	workerInfo.WrittenKey = make(map[string]int)
	workerInfo.IntermediateFileWriteRequestToken = ""
	workerInfo.IntermediateFileWriteRequestView = nil

	workerInfo.Lock.Unlock()

	reply.Flag = true
	reply.ErrStr = ""

	return nil
}
