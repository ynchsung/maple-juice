package common

type ArgGrep struct {
	Paths      []string
	RequestStr string
	IsRegex    bool
}

type ArgWriteFile struct {
	Path    string
	Content []byte
}

type ArgGetMachineID int

type ArgGetMemberList int

type ArgClientMemberJoin int

type ArgClientMemberLeave int

type ArgMemberJoin HostInfo

type ArgMemberAdd HostInfo

type ArgMemberLeave HostInfo

type ArgMemberFailure struct {
	MonitorInfo HostInfo
	FailureInfo HostInfo
}
