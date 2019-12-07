package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"

	"ycsw/common"
)

func maple(line string) []common.MapReduceKeyValue {
	ret := make([]common.MapReduceKeyValue, 0)

	ret = append(ret, common.MapReduceKeyValue{line, "1"})

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
		res := maple(text[0 : N-1])
		output = append(output, res...)
	}

	str, _ := json.Marshal(output)
	fmt.Println(string(str))
}
