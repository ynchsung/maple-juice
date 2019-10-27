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

type ArgClientPutFile struct {
	Filename string
	Length   int
	Content  []byte
}

type ArgClientGetFile struct {
	Filename string
}

type ArgClientDeleteFile struct {
	Filename string
}

type ArgMemberJoin HostInfo

type ArgMemberAdd HostInfo

type ArgMemberLeave HostInfo

type ArgMemberFailure struct {
	MonitorInfo HostInfo
	FailureInfo HostInfo
}

type ArgPutFile struct {
	Filename string
	Length   int
	Content  []byte
}

type ArgGetFile struct {
	Filename string
}

type ArgDeleteFile struct {
	Filename string
}
