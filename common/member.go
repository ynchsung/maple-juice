package common

import (
	"sync"
	"time"
)

type MemberInfo struct {
	Info      HostInfo
	Timestamp time.Time
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

func AddMember(info HostInfo) error {
	MemberListMux.Lock()
	defer MemberListMux.Unlock()

	now := time.Now()
	MemberList = append(MemberList, MemberInfo{info, now})

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
	defer MemberListMux.Unlock()

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
