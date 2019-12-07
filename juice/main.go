package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"ycsw/common"
)

func juice(key string, list []string) []common.MapReduceKeyValue {
	ret := make([]common.MapReduceKeyValue, 0)

	sum := 0

	for _, value := range list {
		v, _ := strconv.Atoi(value)
		sum += v
	}

	ret = append(ret, common.MapReduceKeyValue{key, strconv.Itoa(sum)})

	return ret
}

func main() {
	input := make([]common.MapReduceKeyValue, 0)

	_ = json.NewDecoder(os.Stdin).Decode(&input)

	list := make([]string, 0)
	key := ""
	for _, obj := range input {
		key = obj.Key
		list = append(list, obj.Value)
	}

	output := juice(key, list)

	str, _ := json.Marshal(output)
	fmt.Println(string(str))
}
