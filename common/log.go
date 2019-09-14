package common

import (
	"log"
	"os"
)

func InitLog(path string) error {
	fp, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	log.SetOutput(fp)
	return nil
}
