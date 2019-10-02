package common

import (
	"fmt"
	"log"
	"sync"
	"time"
)

type MemberInfo struct {
	Info      HostInfo  `json:"info"`
	Incar     int       `json:"incarnation"`
	Timestamp time.Time `json:"timestamp"`
}

var (
	MemberList    []MemberInfo
	MemberListMux sync.Mutex
)

func GetMemberList() []MemberInfo {
	MemberListMux.Lock()
	defer MemberListMux.Unlock()

	ret := make([]MemberInfo, len(MemberList))
	copy(ret, MemberList)
	return ret
}

func GetHeartbeatReceivers(back int, ahead int) []MemberInfo {
	MemberListMux.Lock()
	defer MemberListMux.Unlock()

	N := len(MemberList)
	m := make(map[string]MemberInfo)
	for i, mem := range MemberList {
		if mem.Info.Host == Cfg.Self.Host {
			for j := 1; j <= back; j++ {
				m[MemberList[(i-j+N)%N].Info.Host] = MemberList[(i-j+N)%N]
			}

			for j := 1; j <= ahead; j++ {
				m[MemberList[(i+j)%N].Info.Host] = MemberList[(i+j)%N]
			}

			ret := []MemberInfo{}
			for k, v := range m {
				if k != Cfg.Self.Host {
					ret = append(ret, v)
				}
			}

			return ret
		}
	}

	log.Printf("[Error] Cannot find heartbeat receivers, this should not happen")
	return nil
}

func AddMember(info HostInfo) error {
	MemberListMux.Lock()
	defer func() {
		for _, mem := range MemberList {
			fmt.Printf("Host %v, id %v, timestamp %v\n", mem.Info.Host, mem.Info.MachineID, mem.Timestamp)
		}
		fmt.Printf("\n")
		MemberListMux.Unlock()
	}()

	now := time.Now()
	MemberList = append(MemberList, MemberInfo{info, 0, now})

	pos := len(MemberList) - 1
	for {
		if pos == 0 {
			break
		}

		if MemberList[pos-1].Info.MachineID > MemberList[pos].Info.MachineID {
			MemberList[pos-1], MemberList[pos] = MemberList[pos], MemberList[pos-1]
		} else {
			break
		}
		pos -= 1
	}

	return nil
}

func DeleteMember(info HostInfo) error {
	MemberListMux.Lock()
	defer func() {
		for _, mem := range MemberList {
			fmt.Printf("Host %v, id %v, timestamp %v\n", mem.Info.Host, mem.Info.MachineID, mem.Timestamp)
		}
		fmt.Printf("\n")
		MemberListMux.Unlock()
	}()

	for i, mem := range MemberList {
		if mem.Info.MachineID == info.MachineID {
			for j := i; j < len(MemberList)-1; j++ {
				MemberList[j] = MemberList[j+1]
			}
			MemberList = MemberList[:len(MemberList)-1]
			break
		}
	}

	return nil
}

func UpdateHeartbeat(info MemberInfo, now time.Time) bool {
	MemberListMux.Lock()
	defer MemberListMux.Unlock()

	for i := 0; i < len(MemberList); i++ {
		if MemberList[i].Info.MachineID == info.Info.MachineID {
			if MemberList[i].Incar < info.Incar {
				MemberList[i].Incar = info.Incar
				MemberList[i].Timestamp = now
				return true
			}
			return false
		}
	}
	return false
}
