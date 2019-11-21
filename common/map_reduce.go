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
	State           int
	WorkerList      []MemberInfo
	DispatchFileMap map[string][]string
	Counter         map[string]int
	Lock            sync.RWMutex
}

type MapWorkerInfo struct {
	State            int
	ExecFilePath     string
	MasterHost       HostInfo
	WorkerNum        int
	WorkerList       []MemberInfo
	KeyValueReceived map[string][]MapReduceKeyValue
	KeyValueSent     map[string][]MapReduceKeyValue
	Lock             sync.RWMutex
}

const (
	MASTER_STATE_NONE                       = 0
	MASTER_STATE_PREPARE                    = 1
	MASTER_STATE_WAIT_FOR_MAP_TASK          = 2
	MASTER_STATE_WAIT_FOR_INTERMEDIATE_FILE = 3
	MASTER_STATE_MAP_TASK_DONE              = 4
)

var (
	masterInfo MapMasterInfo = MapMasterInfo{
		MASTER_STATE_NONE,
		make([]MemberInfo, 0),
		make(map[string][]string),
		make(map[string]int),
		sync.RWMutex{},
	}
	workerInfo MapWorkerInfo = MapWorkerInfo{
		-1,
		"",
		HostInfo{},
		0,
		make([]MemberInfo, 0),
		make(map[string][]MapReduceKeyValue),
		make(map[string][]MapReduceKeyValue),
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
	workerNum := workerInfo.WorkerNum
	workerInfo.Lock.RUnlock()

	// process input file
	sendArray := make([][]MapReduceKeyValue, workerNum)
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
			idx := SDFSHash2(obj.Key, uint32(workerNum))
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
		for ; it <= worker.Info.MachineID && it < workerNum; it++ {
			sendTmp = append(sendTmp, sendArray[it]...)
		}
		workerInfo.KeyValueSent[worker.Info.Host] = append(workerInfo.KeyValueSent[worker.Info.Host], sendTmp...)

		task := &RpcAsyncCallerTask{
			"MapTaskSendKeyValues",
			worker.Info,
			&ArgMapTaskSendKeyValues{sendTmp}, // TODO
			new(ReplyMapTaskSendKeyValues),    // TODO
			make(chan error),
		}

		go CallRpcS2SGeneral(task)

		tasks = append(tasks, task)
	}

	worker := workerInfo.WorkerList[0]
	sendTmp := make([]MapReduceKeyValue, 0)
	for ; it < workerNum; it++ {
		sendTmp = append(sendTmp, sendArray[it]...)
	}
	workerInfo.KeyValueSent[worker.Info.Host] = append(workerInfo.KeyValueSent[worker.Info.Host], sendTmp...)

	task := &RpcAsyncCallerTask{
		"MapTaskSendKeyValues",
		worker.Info,
		&ArgMapTaskSendKeyValues{sendTmp}, // TODO
		new(ReplyMapTaskSendKeyValues),    // TODO
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
		&ArgMapTaskNotifyMaster{},     // TODO
		new(ReplyMapTaskNotifyMaster), // TODO
		make(chan error),
	}

	go CallRpcS2SGeneral(task)

	err = <-task.Chan
	if err != nil {
		log.Printf("[Error][Map-worker] cannot notify master: %v", err)
	}
}
