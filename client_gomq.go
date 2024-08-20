//go:build !goczmq
// +build !goczmq

package boomer

import (
	"fmt"
	"github.com/sandwich-go/boost/retry"
	"log"
	"sync"

	"github.com/myzhan/gomq"
	"github.com/myzhan/gomq/zmtp"
)

type gomqSocketClient struct {
	masterHost string
	masterPort int
	identity   string

	dealerSocket gomq.Dealer

	fromMaster             chan message
	toMaster               chan message
	disconnectedFromMaster chan bool
	shutdownChan           chan bool
	wg                     sync.WaitGroup
}

func newClient(masterHost string, masterPort int, identity string) (client *gomqSocketClient) {
	log.Println("Boomer is built with gomq support.")
	client = &gomqSocketClient{
		masterHost:             masterHost,
		masterPort:             masterPort,
		identity:               identity,
		fromMaster:             make(chan message, 100),
		toMaster:               make(chan message, 100),
		disconnectedFromMaster: make(chan bool),
	}
	return client
}

func (c *gomqSocketClient) connect() (err error) {
	c.shutdownChan = make(chan bool)
	addr := fmt.Sprintf("tcp://%s:%d", c.masterHost, c.masterPort)
	c.dealerSocket = gomq.NewDealer(zmtp.NewSecurityNull(), c.identity)

	if err = c.dealerSocket.Connect(addr); err != nil {
		log.Println("Error connecting to master:", err)
		return err
	}

	log.Printf("Boomer is connected to master(%s) press Ctrl+c to quit.\n", addr)
	c.wg.Add(2)
	go c.recv()
	go c.send()

	return nil
}

func (c *gomqSocketClient) reconnect() error {
	c.close()
	return retry.Do(
		func(attempt uint) error {
			return c.connect()
		},
		retry.WithOnRetry(func(attempt uint, err error) {
			log.Printf("Attempt %d to reconnect to master: %s\n", attempt, err)
		}),
	)
}

func (c *gomqSocketClient) close() {
	close(c.shutdownChan)
	if c.dealerSocket != nil {
		c.dealerSocket.Close()
	}
	c.wg.Wait() // 等待 send 和 recv 退出
}

func (c *gomqSocketClient) recvChannel() chan message {
	return c.fromMaster
}

func (c *gomqSocketClient) recv() {
	defer c.wg.Done()
	for {
		select {
		case <-c.shutdownChan:
			return
		case msg := <-c.dealerSocket.RecvChannel():
			if msg.MessageType == zmtp.CommandMessage {
				continue
			}
			if len(msg.Body) == 0 {
				continue
			}
			body, err := msg.Body[0], msg.Err
			if err != nil {
				log.Printf("Error reading: %v, attempting to reconnect...\n", err)
				go func() {
					if err := c.reconnect(); err != nil {
						log.Printf("Reconnection failed: %v\n", err)
					}
				}()
				continue
			}
			decodedMsg, err := newGenericMessageFromBytes(body)
			if err != nil {
				log.Printf("Msgpack decode fail: %v\n", err)
				continue
			}
			if decodedMsg.NodeID != c.identity {
				log.Printf("Recv a %s message for node(%s), not for me(%s), dropped.\n", decodedMsg.Type, decodedMsg.NodeID, c.identity)
				continue
			}
			c.fromMaster <- decodedMsg
		}
	}
}

func (c *gomqSocketClient) sendChannel() chan message {
	return c.toMaster
}

func (c *gomqSocketClient) send() {
	defer c.wg.Done()
	for {
		select {
		case <-c.shutdownChan:
			return
		case msg := <-c.toMaster:
			err := c.sendMessage(msg)
			if err != nil {
				log.Printf("Error sending: %v, attempting to reconnect...\n", err)
				go func() {
					if err := c.reconnect(); err != nil {
						log.Printf("Reconnection failed: %v\n", err)
					}
				}()
				continue
			}
			// We may send genericMessage or clientReadyMessage to master.
			m, ok := msg.(*genericMessage)
			if ok {
				if m.Type == "quit" {
					c.disconnectedFromMaster <- true
				}
			}
		}
	}
}

func (c *gomqSocketClient) sendMessage(msg message) error {
	serializedMessage, err := msg.serialize()
	if err != nil {
		log.Printf("Msgpack encode fail: %v\n", err)
		return err
	}
	err = c.dealerSocket.Send(serializedMessage)
	if err != nil {
		log.Printf("Error sending: %v\n", err)
	}
	return err
}

func (c *gomqSocketClient) disconnectedChannel() chan bool {
	return c.disconnectedFromMaster
}
