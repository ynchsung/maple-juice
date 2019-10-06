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

func InMemberList(host HostInfo) bool {
	MemberListMux.Lock()
	defer MemberListMux.Unlock()

	for _, mem := range MemberList {
		if mem.Info.Host == host.Host {
			return true
		}
	}

	return false
}

func EraseMemberList() {
	MemberListMux.Lock()
	defer func() {
		fmt.Println("Empty member list")
		MemberListMux.Unlock()
	}()

	MemberList = []MemberInfo{}
}

func GetHeartbeatReceivers(back int, ahead int) map[string]MemberInfo {
	MemberListMux.Lock()
	defer MemberListMux.Unlock()

	N := len(MemberList)
	receiverMap := make(map[string]MemberInfo)

	if N == 0 {
		return receiverMap
	}

	for i, mem := range MemberList {
		if mem.Info.Host == Cfg.Self.Host {
			for j := 1; j <= back; j++ {
				receiverMap[MemberList[(i-j+N)%N].Info.Host] = MemberList[(i-j+N)%N]
			}

			for j := 1; j <= ahead; j++ {
				receiverMap[MemberList[(i+j)%N].Info.Host] = MemberList[(i+j)%N]
			}

			if _, ok := receiverMap[Cfg.Self.Host]; ok {
				delete(receiverMap, Cfg.Self.Host)
			}

			return receiverMap
		}
	}

	log.Printf("[Error] Cannot find heartbeat receivers, this should not happen")
	return receiverMap
}

func PrepareHeartbeatInfoForMonitor(back int, ahead int) (map[string]MemberInfo, []MemberInfo) {
	MemberListMux.Lock()
	defer MemberListMux.Unlock()

	now := time.Now()
	N := len(MemberList)
	senderMap := make(map[string]MemberInfo)
	memberListCopy := make([]MemberInfo, N)

	if N == 0 {
		return senderMap, memberListCopy
	}

	copy(memberListCopy, MemberList)

	for i, mem := range MemberList {
		if mem.Info.Host == Cfg.Self.Host {
			for j := 1; j <= ahead; j++ {
				senderMap[MemberList[(i-j+N)%N].Info.Host] = MemberList[(i-j+N)%N]
			}

			for j := 1; j <= back; j++ {
				senderMap[MemberList[(i+j)%N].Info.Host] = MemberList[(i+j)%N]
			}

			if _, ok := senderMap[Cfg.Self.Host]; ok {
				delete(senderMap, Cfg.Self.Host)
			}

			// update non-sender timestamp to avoid
			// false detection on new senders when the member list changes
			for j := 0; j < len(MemberList); j++ {
				if _, ok := senderMap[MemberList[j].Info.Host]; !ok {
					MemberList[j].Timestamp = now
				}
			}

			return senderMap, memberListCopy
		}
	}

	log.Printf("[Error] Cannot find heartbeat senders, this should not happen")
	return senderMap, memberListCopy
}

func AddMember(info HostInfo) error {
	MemberListMux.Lock()
	defer func() {
		fmt.Printf("\n[AddMember] MemberList change\n==\n")
		for _, mem := range MemberList {
			fmt.Printf("Host %v, id %v, timestamp %v\n", mem.Info.Host, mem.Info.MachineID, mem.Timestamp.Unix())
		}
		fmt.Printf("\n\n")
		MemberListMux.Unlock()
	}()

	now := time.Now()
	MemberList = append(MemberList, MemberInfo{info, 0, now})

	for pos := len(MemberList) - 1; pos > 0; pos-- {
		if MemberList[pos-1].Info.MachineID > MemberList[pos].Info.MachineID {
			MemberList[pos-1], MemberList[pos] = MemberList[pos], MemberList[pos-1]
		} else {
			break
		}
	}

	return nil
}

func DeleteMember(info HostInfo) error {
	MemberListMux.Lock()
	defer func() {
		fmt.Printf("\n[DeleteMember] MemberList change\n==\n")
		for _, mem := range MemberList {
			fmt.Printf("Host %v, id %v, timestamp %v\n", mem.Info.Host, mem.Info.MachineID, mem.Timestamp.Unix())
		}
		fmt.Printf("\n\n")
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

func UpdateHeartbeat(info HostInfo, incar int, now time.Time) bool {
	MemberListMux.Lock()
	defer MemberListMux.Unlock()

	for i := 0; i < len(MemberList); i++ {
		if MemberList[i].Info.MachineID == info.MachineID {
			if MemberList[i].Incar < incar {
				MemberList[i].Incar = incar
				MemberList[i].Timestamp = now
				return true
			}
			return false
		}
	}
	return false
}
