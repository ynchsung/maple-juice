package common

import (
	"fmt"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	// init
	err := ReadClusterConfig("cluster.json")
	if err != nil {
		fmt.Printf("Read cluster file error\n")
		os.Exit(1)
	}

	// just use 3 machines for unit test
	Cfg.ClusterInfo.Hosts = Cfg.ClusterInfo.Hosts[:3]

	os.Exit(m.Run())
}

func TestGrepFile(t *testing.T) {
	const (
		TEST_LOG_PATH   string = "test.log"
		TEST_GREP_REGEX string = "08:52:[0-9]{2}"
	)

	var (
		chans1 []chan error
		chans2 []chan error

		argsList []ArgWriteFile = []ArgWriteFile{
			ArgWriteFile{
				TEST_LOG_PATH,
				[]byte("03/22 08:51:06 INFO   :.....mailslot_create: creating mailslot for RSVP\n03/22 08:51:06 INFO   :....mailbox_register: mailbox allocated for rsvp\n03/22 08:51:06 INFO   :.....mailslot_create: creating mailslot for RSVP via UDP"),
			},
			ArgWriteFile{
				TEST_LOG_PATH,
				[]byte("03/22 08:52:50 TRACE  :......rsvp_event_establishSession: local node will send\n03/22 08:52:50 INFO   :........router_forward_getOI: Ioctl to get route entry successful\n03/22 08:52:50 TRACE  :........router_forward_getOI:         source address:   9.67.116.98\n03/22 08:52:50 TRACE  :........router_forward_getOI:         out inf:   9.67.116.98"),
			},
			ArgWriteFile{
				TEST_LOG_PATH,
				[]byte("03/22 08:52:52 TRACE  :.......rsvp_flow_stateMachine: entering state RESVED\n03/22 08:53:07 EVENT  :..mailslot_sitter: process received signal SIGALRM\n03/22 08:53:07 INFO   :......router_forward_getOI: Ioctl to query route entry successful\n03/22 08:53:07 TRACE  :......router_forward_getOI:         source address:   9.67.116.98\n03/22 08:53:07 TRACE  :......router_forward_getOI:         out inf:   9.67.116.98\n03/22 08:53:07 TRACE  :......router_forward_getOI:         gateway:   0.0.0.0\n03/22 08:53:07 TRACE  :......router_forward_getOI:         route handle:   7f5251c8\n03/22 08:53:07 INFO   :......rsvp_flow_stateMachine: state RESVED, event T1OUT"),
			},
		}
		replyList []*ReplyWriteFile
	)

	var answer []GrepInfo = []GrepInfo{
		GrepInfo{
			TEST_LOG_PATH,
			[]Line{},
		},
		GrepInfo{
			TEST_LOG_PATH,
			[]Line{
				Line{
					1,
					"03/22 08:52:50 TRACE  :......rsvp_event_establishSession: local node will send",
				},
				Line{
					2,
					"03/22 08:52:50 INFO   :........router_forward_getOI: Ioctl to get route entry successful",
				},
				Line{
					3,
					"03/22 08:52:50 TRACE  :........router_forward_getOI:         source address:   9.67.116.98",
				},
				Line{
					4,
					"03/22 08:52:50 TRACE  :........router_forward_getOI:         out inf:   9.67.116.98",
				},
			},
		},
		GrepInfo{
			TEST_LOG_PATH,
			[]Line{
				Line{
					1,
					"03/22 08:52:52 TRACE  :.......rsvp_flow_stateMachine: entering state RESVED",
				},
			},
		},
	}

	// Distribute test logs
	for i, host := range Cfg.ClusterInfo.Hosts {
		r := &ReplyWriteFile{true, "", 0}
		c := make(chan error)
		go CallRpcS2SWriteFile(host.Host, host.Port, &argsList[i], r, c)

		replyList = append(replyList, r)
		chans1 = append(chans1, c)
	}

	for i, _ := range Cfg.ClusterInfo.Hosts {
		_ = <-chans1[i]
	}

	// Send RpcS2S Grep File
	var (
		args  ArgGrep = ArgGrep{[]string{TEST_LOG_PATH}, TEST_GREP_REGEX, true}
		reply ReplyGrepList
	)

	for _, host := range Cfg.ClusterInfo.Hosts {
		r := new(ReplyGrep)
		c := make(chan error)
		go CallRpcS2SGrepFile(host.Host, host.Port, &args, r, c)

		reply = append(reply, r)
		chans2 = append(chans2, c)
	}

	for i, _ := range Cfg.ClusterInfo.Hosts {
		_ = <-chans2[i]
	}

	// Compare answer
	if len(answer) != len(reply) {
		t.Errorf("Reply length error: %v vs %v", len(answer), len(reply))
	}

	for i, replyObj := range reply {
		if !replyObj.Flag {
			fmt.Printf("Host %v down, skip it\n", Cfg.ClusterInfo.Hosts[i].Host)
			continue
		}

		if len(replyObj.Files) != 1 {
			t.Errorf("Host %v error: there should be only one file (but get %v file(s) now)", Cfg.ClusterInfo.Hosts[i].Host, len(replyObj.Files))
		}

		if len(answer[i].Lines) != len(replyObj.Files[0].Lines) {
			t.Errorf("Host %v got wrong number of lines: %v vs %v", Cfg.ClusterInfo.Hosts[i].Host, len(answer[i].Lines), len(replyObj.Files[0].Lines))
		}

		for j, line := range answer[i].Lines {
			if line.LineNum != replyObj.Files[0].Lines[j].LineNum || line.LineStr != replyObj.Files[0].Lines[j].LineStr {
				t.Errorf("Host %v got wrong grep content: #%v\n", Cfg.ClusterInfo.Hosts[i].Host, line.LineNum)
			}
		}
	}
}
