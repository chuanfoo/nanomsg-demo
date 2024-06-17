package main

import (
	"flag"
	"fmt"
	"os"

	_ "go.nanomsg.org/mangos/v3/transport/all"

	"mynanoclient/messaging"
)

func main() {
	var ip, port, channel string
	flag.StringVar(&ip, "ip", "", "nano服务ip")
	flag.StringVar(&port, "p", "", "nano服务端口")
	flag.StringVar(&channel, "c", "", "频道")
	flag.Parse()

	if ip == "" {
		fmt.Println("缺少-nano服务ip")
		return
	}
	if port == "" {
		fmt.Println("缺少-nano服务端口")
		return
	}
	if channel == "" {
		fmt.Println("缺少-频道")
		return
	}

	messaging.InitConsumers(fmt.Sprintf("tcp://%s:%s", ip, port))

	sign := make(chan os.Signal)

	messaging.SubscribeToTopicAll(channel, func(msgTopic, msgStr string) {
		// 不处理新撮合的币对
		fmt.Println(msgStr)
	})
	<-sign
}
