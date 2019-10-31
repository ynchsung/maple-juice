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

type ArgClientUpdateFile struct {
	Filename   string
	DeleteFlag bool
	Length     int
	Content    []byte
	ForceFlag  bool
}

type ArgClientGetFile struct {
	Filename string
}

type ArgMemberJoin HostInfo

type ArgMemberAdd HostInfo

type ArgMemberLeave HostInfo

type ArgMemberFailure struct {
	MonitorInfo HostInfo
	FailureInfo HostInfo
}

type ArgUpdateFileVersion struct {
	Filename  string
	ForceFlag bool
}

type ArgUpdateFile struct {
	Filename   string
	DeleteFlag bool
	Version    int
	Length     int
	Content    []byte
}

type ArgGetFile struct {
	Filename string
}
