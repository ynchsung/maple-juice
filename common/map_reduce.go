package common

import (
	"encoding/json"
	"io"
	"log"
	"os/exec"
	"sync"
)

type MapReduceKeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type MapMasterInfo struct {
	State                       int
	WorkerList                  []MemberInfo
	WorkerMap                   map[string]MemberInfo
	DispatchFileMap             map[string][]string
	FinishFileCounter           map[string]int
	IntermediateDoneNotifyToken string
	IntermediateDoneCounter     map[string]int
	Lock                        sync.RWMutex
}

type MapWorkerInfo struct {
	State                      int
	ExecFilePath               string
	IntermediateFilenamePrefix string
	MasterHost                 HostInfo
	InitWorkerNum              int
	WorkerList                 []MemberInfo
	WorkerMap                  map[string]MemberInfo
	KeyValueReceived           map[string]map[string][]MapReduceKeyValue // key -> input_filename -> list of K-Vs
	KeyValueSent               map[string]map[string][]MapReduceKeyValue // host -> input_filename -> list of K-Vs
	WrittenKey                 map[string]int
	Lock                       sync.RWMutex
}

const (
	MASTER_STATE_NONE                       = 0
	MASTER_STATE_PREPARE                    = 1
	MASTER_STATE_WAIT_FOR_MAP_TASK          = 2
	MASTER_STATE_WAIT_FOR_INTERMEDIATE_FILE = 3
	MASTER_STATE_MAP_TASK_DONE              = 4
)

const (
	NOTIFY_TYPE_FINISH_MAP_TASK          = 0
	NOTIFY_TYPE_FINISH_INTERMEDIATE_FILE = 1
)

var (
	masterInfo MapMasterInfo = MapMasterInfo{
		MASTER_STATE_NONE,
		make([]MemberInfo, 0),
		make(map[string]MemberInfo),
		make(map[string][]string),
		make(map[string]int),
		"",
		make(map[string]int),
		sync.RWMutex{},
	}
	workerInfo MapWorkerInfo = MapWorkerInfo{
		-1,
		"",
		"",
		HostInfo{},
		0,
		make([]MemberInfo, 0),
		make(map[string]MemberInfo),
		make(map[string]map[string][]MapReduceKeyValue),
		make(map[string]map[string][]MapReduceKeyValue),
		make(map[string]int),
		sync.RWMutex{},
	}
)

func MapTask(filename string) {
	// download input file from sdfs
	content, length, err := SDFSDownloadFile(filename, Cfg.Self)
	if err != nil {
		log.Printf("[Error][Map-worker] cannot get input file %v: %v", filename, err)
		return
	}

	workerInfo.Lock.RLock()
	initWorkerNum := workerInfo.InitWorkerNum
	workerInfo.Lock.RUnlock()

	// process input file
	sendArray := make([][]MapReduceKeyValue, initWorkerNum)
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

		cmd := exec.Command(workerInfo.ExecFilePath)
		stdin, _ := cmd.StdinPipe()

		go func() {
			defer stdin.Close()
			io.WriteString(stdin, string(content[start:i]))
		}()

		out, _ := cmd.Output()
		var outputKeyValue []MapReduceKeyValue
		_ = json.Unmarshal(out, &outputKeyValue)

		for _, obj := range outputKeyValue {
			idx := SDFSHash2(obj.Key, uint32(initWorkerNum))
			sendArray[idx] = append(sendArray[idx], obj)
		}

		start = i
		if start >= length {
			break
		}
	}

	// send key value to other workers for collecting
	tasks := make([]*RpcAsyncCallerTask, 0)
	workerInfo.Lock.Lock()
	master := workerInfo.MasterHost
	it := 0
	for _, worker := range workerInfo.WorkerList {
		sendTmp := make([]MapReduceKeyValue, 0)
		for ; it <= worker.Info.MachineID && it < initWorkerNum; it++ {
			sendTmp = append(sendTmp, sendArray[it]...)
		}
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

	worker := workerInfo.WorkerList[0]
	sendTmp := make([]MapReduceKeyValue, 0)
	for ; it < initWorkerNum; it++ {
		sendTmp = append(sendTmp, sendArray[it]...)
	}
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

	workerInfo.Lock.Unlock()

	// Wait for all RpcAsyncCallerTask
	for _, task := range tasks {
		<-task.Chan
	}

	// send MapTaskNotifyMaster
	task = &RpcAsyncCallerTask{
		"MapTaskNotifyMaster",
		master,
		&ArgMapTaskNotifyMaster{Cfg.Self, NOTIFY_TYPE_FINISH_MAP_TASK, ""},
		new(ReplyMapTaskNotifyMaster),
		make(chan error),
	}

	go CallRpcS2SGeneral(task)

	err = <-task.Chan
	if err != nil {
		log.Printf("[Error][Map-worker] cannot notify master: %v", err)
	}
}

func MapTaskWriteIntermediateFiles(token string) {
	bucket := make(map[string][]MapReduceKeyValue)

	workerInfo.Lock.Lock()

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

	prefix := workerInfo.IntermediateFilenamePrefix
	master := workerInfo.MasterHost

	workerInfo.Lock.Unlock()

	chans := make([]chan error, 0)
	for k, l := range bucket {
		c := make(chan error)
		chans = append(chans, c)

		go func(ch chan error, key string, list []MapReduceKeyValue) {
			filename := prefix + "_" + key
			content, _ := json.Marshal(list)
			err := SDFSUploadFile(filename, Cfg.Self, content)
			if err != nil {
				log.Printf("[Error][Map-worker] cannot write intermediate file %v: %v", filename, err)
			}

			ch <- nil
		}(c, k, l)
	}

	for _, c := range chans {
		<-c
	}

	// send MapTaskNotifyMaster
	task := &RpcAsyncCallerTask{
		"MapTaskNotifyMaster",
		master,
		&ArgMapTaskNotifyMaster{Cfg.Self, NOTIFY_TYPE_FINISH_INTERMEDIATE_FILE, token},
		new(ReplyMapTaskNotifyMaster),
		make(chan error),
	}

	go CallRpcS2SGeneral(task)

	err := <-task.Chan
	if err != nil {
		log.Printf("[Error][Map-worker] cannot notify master: %v", err)
	}
}
