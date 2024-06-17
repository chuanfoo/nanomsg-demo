package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"mynanoclient/messaging"
	"os"
	"strconv"
	"time"
)

type ChannelMsg struct {
	Channel string `json:"channel"`
}

func main() {
	var port, file string
	var t int
	flag.StringVar(&port, "p", "", "nano服务端口")
	flag.IntVar(&t, "t", 0, "频率s")
	flag.StringVar(&file, "f", "", "文件")
	flag.Parse()

	if port == "" {
		fmt.Println("缺少-nano服务端口")
		return
	}

	if t == 0 {
		t = 2
	}

	if file == "" {
		file = "testdata.txt"
	}

	xport, err := strconv.ParseInt(port, 10, 64)
	if err != nil {
		fmt.Println(err)
		return
	}

	messaging.InitServer(int(xport))

	msgs, err := ReadLines(file)
	if err != nil {
		fmt.Println(err)
		return
	}
	for {
		for _, msg := range msgs {
			var chMsg ChannelMsg
			err := json.Unmarshal([]byte(msg), &chMsg)
			if err != nil {
				fmt.Println(err)
				continue
			}
			messaging.Publish(chMsg.Channel, msg)
			time.Sleep(time.Duration(t) * time.Second)
		}
	}
}

// ReadLines reads all lines of the file.
func ReadLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}
