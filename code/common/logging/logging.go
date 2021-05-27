package logging

import (
	"fmt"
	"log"
)

var logtag string = ""

func Initialize(tag string) {
	logtag = tag
}

func Log(msg string) {
	message := fmt.Sprintf("[%s] %s", logtag, msg)
	log.Println(message)
}

func LogError(msg string, err error) {
	message := fmt.Sprintf("%s : %s", msg, err.Error())
	Log(message)
}
