package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"

	"ycsw/common"
)

func maple(line string) []common.MapReduceKeyValue {
	ret := make([]common.MapReduceKeyValue, 0)

	strs := regexp.MustCompile("\t| ").Split(line, -1)
	re := regexp.MustCompile("^[a-zA-Z]+$")
	for _, str := range strs {
		if len(str) > 0 && re.MatchString(str) {
			ret = append(ret, common.MapReduceKeyValue{strings.ToLower(str), "1"})
		}
	}

	return ret
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	output := make([]common.MapReduceKeyValue, 0)
	for {
		text, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		N := len(text)
		if N >= 2 && text[N-2] == '\r' {
			N -= 1
		}
		res := maple(text[0 : N-1])
		output = append(output, res...)
	}

	str, _ := json.Marshal(output)
	fmt.Println(string(str))
}
