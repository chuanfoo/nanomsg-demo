package messaging

import "C"
import (
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/sub"
	_ "go.nanomsg.org/mangos/v3/transport/all"
)

var consumers *sync.Map

func InitConsumers(urls string) {
	consumers = new(sync.Map)
	for _, url := range strings.Split(urls, ",") {
		consumer := newConsumer(url)
		if err := consumer.Connect(); err != nil {
			logrus.Errorf("nano consumer Connect error url=%s, error=%s", url, err.Error())
			os.Exit(1)
		}
		consumers.Store(url, consumer)
		logrus.Infof("consumer Connect after url=%s SUCC", url)
	}
}

func SubscribeToTopicAll(topic string, consumeFunc func(msgTopic, msg string)) {
	consumers.Range(func(key, value interface{}) bool {
		consumer := value.(*consumer)
		if err := consumer.SubscribeToTopic(topic, consumeFunc); err != nil {
			logrus.Errorf("subscribe msg server[%s] topic[%s] fail", key, topic)
		}
		logrus.Debugf("subscribe msg successful topic=%s", topic)
		return true
	})
}

func UnSubscribeToTopicAll(topic string) {
	consumers.Range(func(key, value interface{}) bool {
		consumer := value.(*consumer)
		consumer.UnSubscribeToTopic(topic)
		logrus.Infof("unSubscribe topic[%s] successfully.", topic)
		return true
	})
}

type consumer struct {
	url  string
	sock mangos.Socket

	// 使用topics需要加锁， 线程不安全
	topics      map[string]func(str1, str2 string)
	topicsMutex sync.RWMutex
}

func newConsumer(url string) *consumer {
	return &consumer{
		url:    url,
		topics: make(map[string]func(str1, str2 string), 1024),
	}
}

//Starts listening for Subscriptions on the specified url.

func (self *consumer) Connect() error {

	var err error
	if self.sock, err = sub.NewSocket(); err != nil {
		return err
	}

	if err = self.sock.Dial(self.url); err != nil {
		return err
	}
	//
	//if err = self.sock.Listen("tcp://0.0.0.0:5555"); err != nil {
	//	return err
	//}

	// Empty byte array effectively subscribes to everything
	//err = self.sock.SetOption(mangos.OptionSubscribe, []byte(""))
	//if err != nil {
	//	return err
	//}

	self.start()
	return nil
}

func (self *consumer) start() {
	// 接受消息
	go func() {
		for {
			if err := self.consume(); err != nil {
				logrus.Errorf("recv error:%s", err.Error())
				time.Sleep(time.Second)
			}
		}
	}()
}

func (self *consumer) SubscribeToTopic(topic string, consumeFunc func(topic, msg string)) error {
	var err error
	if consumeFunc == nil {
		return errors.New("consumeFunc is nil")
	}

	if err = self.sock.SetOption(mangos.OptionSubscribe, []byte(topic)); err != nil {
		return err
	}
	self.topicsMutex.Lock()
	defer self.topicsMutex.Unlock()
	self.topics[topic] = consumeFunc
	return nil
}

func (self *consumer) UnSubscribeToTopic(topic string) {
	self.topicsMutex.Lock()
	defer self.topicsMutex.Unlock()
	delete(self.topics, topic)
}

func (self *consumer) consume() error {
	defer func() {
		if err := recover(); err != nil {
			logrus.Errorf("nano Consumer start fail")
			debug.PrintStack()
		}
	}()

	var err error
	var data []byte
	if data, err = self.sock.Recv(); err != nil {
		return err
	}
	_data := string(data)
	logrus.Debugf("RECEIVED data %s\n", _data)
	if msgTemp := strings.SplitN(_data, "|", 2); len(msgTemp) == 2 {
		topic := msgTemp[0]
		msg := msgTemp[1]
		// 此处会接收大量wss数据，如果使用sync.map 将面临加锁和频繁的类型转换问题，因为sync.map 存储的是interface{}
		// 此处读写锁适合
		self.topicsMutex.RLock()
		defer self.topicsMutex.RUnlock()
		if topicValue, ok := self.topics[topic]; ok && topicValue != nil {
			go topicValue(topic, msg)
		} else {
			logrus.Warnf("no subscribe for topic:%s", topic)
		}
	} else {
		return fmt.Errorf("raw data parse error, data:%s", _data)
	}
	return err
}
