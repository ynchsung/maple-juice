package common

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os/exec"
	"sync"
	"time"
)

type MapReduceKeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type WorkerInfo struct {
	Info     HostInfo `json:"info"`
	WorkerID int      `json:"worker_id"`
}

type MapMasterStateInfo struct {
	State      int
	WorkerList []WorkerInfo
	WorkerMap  map[string]WorkerInfo

	// for map task
	DispatchFileMap             map[string][]string
	FinishFileCounter           map[string]int
	IntermediateDoneNotifyToken string
	IntermediateDoneCounter     map[string]int

	// for reduce task
	DispatchFileMap2 map[string][]string
	ResultFileMap    map[string]map[string][]MapReduceKeyValue // worker -> file -> list of key-value

	Lock sync.RWMutex
}

type MapWorkerStateInfo struct {
	State                      int
	ExecFilePath               string
	IntermediateFilenamePrefix string
	MasterHost                 HostInfo
	InitWorkerNum              int
	WorkerList                 []WorkerInfo
	WorkerMap                  map[string]WorkerInfo
	// KeyValueReceived                  map[string]map[string][]MapReduceKeyValue // key -> input_filename -> list of K-Vs
	// KeyValueSent                      map[string]map[string][]MapReduceKeyValue // host -> input_filename -> list of K-Vs
	KeyValueBucket                    []map[string][]MapReduceKeyValue // InitWorkerNum-bucket of map input_filename -> list of K-Vs
	WrittenKey                        map[string]int
	IntermediateFileWriteRequestToken string
	IntermediateFileWriteRequestView  []WorkerInfo
	Lock                              sync.RWMutex
}

const (
	MASTER_STATE_NONE = 0

	MASTER_STATE_MAP_PREPARE                    = 1
	MASTER_STATE_MAP_WAIT_FOR_TASK              = 2
	MASTER_STATE_MAP_WAIT_FOR_INTERMEDIATE_FILE = 3
	MASTER_STATE_MAP_TASK_DONE                  = 4

	MASTER_STATE_REDUCE_PREPARE       = 5
	MASTER_STATE_REDUCE_WAIT_FOR_TASK = 6
	MASTER_STATE_REDUCE_TASK_DONE     = 7
)

const (
	NOTIFY_TYPE_FINISH_MAP_TASK              = 0
	NOTIFY_TYPE_FINISH_MAP_INTERMEDIATE_FILE = 1
)

var (
	masterInfo MapMasterStateInfo = MapMasterStateInfo{
		MASTER_STATE_NONE,
		make([]WorkerInfo, 0),
		make(map[string]WorkerInfo),
		make(map[string][]string),
		make(map[string]int),
		"",
		make(map[string]int),
		make(map[string][]string),
		make(map[string]map[string][]MapReduceKeyValue),
		sync.RWMutex{},
	}
	workerInfo MapWorkerStateInfo = MapWorkerStateInfo{
		-1,
		"",
		"",
		HostInfo{},
		0,
		make([]WorkerInfo, 0),
		make(map[string]WorkerInfo),
		// make(map[string]map[string][]MapReduceKeyValue),
		// make(map[string]map[string][]MapReduceKeyValue),
		make([]map[string][]MapReduceKeyValue, 0),
		make(map[string]int),
		"",
		nil,
		sync.RWMutex{},
	}
)

type MapReduceTaskInfo struct {
	Filename string
	Type     string
}

var (
	MapReduceTaskQueue    []*MapReduceTaskInfo = make([]*MapReduceTaskInfo, 0)
	MapReduceTaskQueueMux sync.Mutex
)

func MapReduceTaskQueueConsumer() {
	for {
		var task *MapReduceTaskInfo = nil

		MapReduceTaskQueueMux.Lock()
		NN := len(MapReduceTaskQueue)
		if NN > 0 {
			task = MapReduceTaskQueue[0]
			MapReduceTaskQueue = MapReduceTaskQueue[1:]
			NN -= 1
			if NN%100 == 0 {
				fmt.Printf("MapReduceTaskQueue size %v\n", NN)
			}
		}
		MapReduceTaskQueueMux.Unlock()

		if task != nil {
			if task.Type == "map" {
				MapTask(task.Filename)
			} else {
				ReduceTask(task.Filename)
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func MapTask(filename string) {
	// download input file from sdfs
	//content, length, err := SDFSDownloadFile(filename, Cfg.Self)
	content, _, err := SDFSDownloadFile(filename, Cfg.Self)
	if err != nil {
		log.Printf("[Error][Map-worker] cannot get input file %v: %v", filename, err)
		return
	}

	workerInfo.Lock.RLock()
	initWorkerNum := workerInfo.InitWorkerNum
	workerInfo.Lock.RUnlock()

	// process input file
	sendArray := make([][]MapReduceKeyValue, initWorkerNum)
	/*
		start := 0
		for {
			i := start
			cnt := 0
			for ; i < length; i++ {
				if content[i] == '\n' {
					cnt += 1
					if cnt >= 10 {
						break
					}
				}
			}
			if i < length {
				i += 1
			}
	*/
	cmd := exec.Command(workerInfo.ExecFilePath)
	stdin, _ := cmd.StdinPipe()

	go func() {
		defer stdin.Close()
		io.WriteString(stdin, string(content))
		//io.WriteString(stdin, string(content[start:i]))
	}()

	out, _ := cmd.Output()
	var outputKeyValue []MapReduceKeyValue
	_ = json.Unmarshal(out, &outputKeyValue)

	for _, obj := range outputKeyValue {
		idx := SDFSHash2(obj.Key, uint32(initWorkerNum))
		sendArray[idx] = append(sendArray[idx], obj)
	}

	/*
		start = i
		if start >= length {
			break
		}
	}*/

	workerInfo.Lock.Lock()
	master := workerInfo.MasterHost

	// put generated keys into memory
	for i, list := range sendArray {
		workerInfo.KeyValueBucket[i][filename] = list
	}

	workerInfo.Lock.Unlock()

	/*
		// send key value to other workers for collecting
		tasks := make([]*RpcAsyncCallerTask, 0)
		workerInfo.Lock.Lock()
		master := workerInfo.MasterHost
		it := 0
		for _, worker := range workerInfo.WorkerList {
			sendTmp := make([]MapReduceKeyValue, 0)
			for ; it <= worker.WorkerID && it < initWorkerNum; it++ {
				sendTmp = append(sendTmp, sendArray[it]...)
			}

			if len(sendTmp) > 0 {
				workerInfo.KeyValueSent[worker.Info.Host][filename] = sendTmp

				task := &RpcAsyncCallerTask{
					"MapTaskSendKeyValues",
					worker.Info,
					&ArgMapTaskSendKeyValues{Cfg.Self, filename, sendTmp},
					new(ReplyMapTaskSendKeyValues),
					make(chan error),
				}

				go CallRpcS2SGeneral(task)

				tasks = append(tasks, task)
			}
		}

		worker := workerInfo.WorkerList[0]
		sendTmp := make([]MapReduceKeyValue, 0)
		for ; it < initWorkerNum; it++ {
			sendTmp = append(sendTmp, sendArray[it]...)
		}

		if len(sendTmp) > 0 {
			workerInfo.KeyValueSent[worker.Info.Host][filename] = append(workerInfo.KeyValueSent[worker.Info.Host][filename], sendTmp...)

			task := &RpcAsyncCallerTask{
				"MapTaskSendKeyValues",
				worker.Info,
				&ArgMapTaskSendKeyValues{Cfg.Self, filename, sendTmp},
				new(ReplyMapTaskSendKeyValues),
				make(chan error),
			}

			go CallRpcS2SGeneral(task)

			tasks = append(tasks, task)
		}

		workerInfo.Lock.Unlock()

		// Wait for all RpcAsyncCallerTask
		for _, task := range tasks {
			<-task.Chan
		}
	*/

	// send MapTaskNotifyMaster
	task := &RpcAsyncCallerTask{
		"MapTaskNotifyMaster",
		master,
		&ArgMapTaskNotifyMaster{Cfg.Self, NOTIFY_TYPE_FINISH_MAP_TASK, "", true},
		new(ReplyMapTaskNotifyMaster),
		make(chan error),
	}

	go CallRpcS2SGeneral(task)

	err = <-task.Chan
	close(task.Chan)
	if err != nil {
		log.Printf("[Error][Map-worker] cannot notify master: %v", err)
	}
}

func MapTaskIntermediateFileWriteThread(prefix string, sendMp map[string][]MapReduceKeyValue, c chan error) {
	cc := 0
	NN := len(sendMp)
	for key, list := range sendMp {
		fn := prefix + "_" + key
		content, _ := json.Marshal(list)

		retry := 0
		for ; retry < 10; retry++ {
			finish, _, err := SDFSUploadFile(Cfg.Self, fn, content, true)
			if err == nil && finish {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}

		if retry == 10 {
			log.Printf("[Error][Map-worker] cannot write intermediate file %v, retry 10 times still fail", fn)
		}

		cc += 1
		if cc%100 == 1 {
			fmt.Printf("write intermediate file %v/%v\n", cc, NN)
		}
	}

	c <- nil
}

func MapTaskWriteIntermediateFiles() {
	token := ""

	for {
		flag := false

		workerInfo.Lock.Lock()
		if len(workerInfo.IntermediateFileWriteRequestView) == len(workerInfo.WorkerList) {
			flag = true
			for i := 0; i < len(workerInfo.IntermediateFileWriteRequestView); i++ {
				if workerInfo.IntermediateFileWriteRequestView[i].Info.Host != workerInfo.WorkerList[i].Info.Host {
					flag = false
					break
				}
			}
		}
		if flag {
			token = workerInfo.IntermediateFileWriteRequestToken
			workerInfo.IntermediateFileWriteRequestToken = ""
			workerInfo.IntermediateFileWriteRequestView = nil
			break
		}
		workerInfo.Lock.Unlock()

		time.Sleep(500 * time.Millisecond)
	}

	/*
		bucket := make(map[string][]MapReduceKeyValue)
		for key, map1 := range workerInfo.KeyValueReceived {
			if _, ok := workerInfo.WrittenKey[key]; ok {
				continue
			}

			if _, ok := bucket[key]; !ok {
				bucket[key] = make([]MapReduceKeyValue, 0)
			}

			for _, list := range map1 {
				bucket[key] = append(bucket[key], list...)
			}
		}

		for key, _ := range bucket {
			workerInfo.WrittenKey[key] = 1
		}
	*/

	target_idx_list := make([]int, 0)
	N := len(workerInfo.WorkerList)
	for i, worker := range workerInfo.WorkerList {
		if worker.Info.Host == Cfg.Self.Host {
			last_worker := workerInfo.WorkerList[(i-1+N)%N]
			for j := (last_worker.WorkerID + 1) % workerInfo.InitWorkerNum; j != worker.WorkerID; j = (j + 1) % workerInfo.InitWorkerNum {
				target_idx_list = append(target_idx_list, j)
			}
			target_idx_list = append(target_idx_list, worker.WorkerID)
			break
		}
	}

	// send MapTaskGetKeyValues (get key values from all workers)
	tasks := make([]*RpcAsyncCallerTask, 0)
	for _, worker := range workerInfo.WorkerList {
		task := &RpcAsyncCallerTask{
			"MapTaskGetKeyValues",
			worker.Info,
			&ArgMapTaskGetKeyValues{target_idx_list},
			new(ReplyMapTaskGetKeyValues),
			make(chan error),
		}

		go CallRpcS2SGeneral(task)

		tasks = append(tasks, task)
	}

	prefix := workerInfo.IntermediateFilenamePrefix
	master := workerInfo.MasterHost

	workerInfo.Lock.Unlock()

	success := true
	// Wait for all RpcAsyncCallerTask
	for _, task := range tasks {
		err := <-task.Chan
		close(task.Chan)
		if err != nil {
			success = false
			log.Printf("[Warn][Map-worker] get key value from %v error: %v (would be recovered later)", task.Info.Host, err)
		}
	}

	if success {
		workerInfo.Lock.Lock()

		bucket := make(map[string][]MapReduceKeyValue)
		for _, task := range tasks {
			r := task.Reply.(*ReplyMapTaskGetKeyValues)
			for _, list := range r.Data {
				for _, obj := range list {
					if _, ok := workerInfo.WrittenKey[obj.Key]; ok {
						continue
					}

					if _, ok := bucket[obj.Key]; !ok {
						bucket[obj.Key] = make([]MapReduceKeyValue, 0)
					}

					bucket[obj.Key] = append(bucket[obj.Key], obj)
				}
			}
		}

		for key, _ := range bucket {
			workerInfo.WrittenKey[key] = 1
		}

		workerInfo.Lock.Unlock()

		thread_num := 64
		sendArr := make([]map[string][]MapReduceKeyValue, thread_num)
		for i := 0; i < thread_num; i++ {
			sendArr[i] = make(map[string][]MapReduceKeyValue)
		}

		fmt.Printf("Intermediate file count %v\n", len(bucket))
		ii := 0
		for key, list := range bucket {
			sendArr[ii][key] = list
			ii = (ii + 1) % thread_num
		}

		chans := make([]chan error, thread_num)
		for i := 0; i < thread_num; i++ {
			chans[i] = make(chan error)
			go MapTaskIntermediateFileWriteThread(prefix, sendArr[i], chans[i])
		}

		for _, c := range chans {
			<-c
			close(c)
		}
	}

	// send MapTaskNotifyMaster
	task := &RpcAsyncCallerTask{
		"MapTaskNotifyMaster",
		master,
		&ArgMapTaskNotifyMaster{Cfg.Self, NOTIFY_TYPE_FINISH_MAP_INTERMEDIATE_FILE, token, success},
		new(ReplyMapTaskNotifyMaster),
		make(chan error),
	}

	go CallRpcS2SGeneral(task)

	err := <-task.Chan
	close(task.Chan)
	if err != nil {
		log.Printf("[Error][Map-worker] cannot notify master: %v", err)
	}
}

func ReduceTask(filename string) {
	// download input file from sdfs
	content, length, err := SDFSDownloadFile(filename, Cfg.Self)
	if err != nil {
		log.Printf("[Error][Reduce-worker] cannot get input file %v: %v", filename, err)
		return
	}

	// process input file
	cmd := exec.Command(workerInfo.ExecFilePath)
	stdin, _ := cmd.StdinPipe()

	go func() {
		defer stdin.Close()
		io.WriteString(stdin, string(content[0:length]))
	}()

	out, _ := cmd.Output()
	var outputKeyValue []MapReduceKeyValue
	_ = json.Unmarshal(out, &outputKeyValue)

	workerInfo.Lock.RLock()
	master := workerInfo.MasterHost
	workerInfo.Lock.RUnlock()

	// send ReduceTaskNotifyMaster
	task := &RpcAsyncCallerTask{
		"ReduceTaskNotifyMaster",
		master,
		&ArgReduceTaskNotifyMaster{Cfg.Self, filename, outputKeyValue},
		new(ReplyReduceTaskNotifyMaster),
		make(chan error),
	}

	go CallRpcS2SGeneral(task)

	err = <-task.Chan
	close(task.Chan)
	if err != nil {
		log.Printf("[Error][Reduce-worker] cannot notify master: %v", err)
	}
}
