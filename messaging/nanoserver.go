package messaging

import (
	"fmt"

	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/pub"
	// register transports
	_ "go.nanomsg.org/mangos/v3/transport/all"
)

var server *Server

func InitServer(port int) {
	server = &Server{url: fmt.Sprintf("tcp://0.0.0.0:%d", port)}
	server.Listen()
}

func Publish(topic, message string) error {
	return server.PublishTopic(topic, message)
}

type Server struct {
	url  string
	sock mangos.Socket
}

// Starts listening for Subscriptions on the specified url.
func (self *Server) Listen() error {

	var err error
	if self.sock, err = pub.NewSocket(); err != nil {
		return err
	}

	if err = self.sock.Listen(self.url); err != nil {
		return err
	}

	return nil
}

// Publish a specific topic to all subscribers.
func (self *Server) PublishTopic(topic string, message string) error {
	err := self.sock.Send([]byte(fmt.Sprintf("%s|%s", topic, message)))
	return err
}
