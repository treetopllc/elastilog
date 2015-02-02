package elastilog

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

const api = "/_bulk?replication=sync"
const Version = "0.0.1"

type Client interface {
	Flush()
	Send(...Entry)
	Close()
}

type client struct {
	tags    []string
	queue   Queue
	hostURI string
}

func NewClient(hostURI string, tags ...string) Client {
	c := &client{
		tags:    tags,
		hostURI: hostURI,
	}
	c.queue = NewQueue(c.writemsgs, 100, time.Millisecond*40)
	return c
}

func (c *client) writemsgs(msgs []Entry) {
	msgBytes := make([][]byte, 0, len(msgs))
	for _, msg := range msgs {
		msg.Tags = append(msg.Tags, c.tags...)
		b, err := msg.BulkString()
		if err != nil {
			fmt.Printf("Error writing log %+v: %v\n", msg, err)
			continue
		}
		msgBytes = append(msgBytes, []byte(b))
	}
	body := bytes.Join(msgBytes, []byte{'\n'})
	body = append(body, '\n')
	res, err := http.Post(c.hostURI+api, "application/json", bytes.NewBuffer(body))
	if err != nil {
		fmt.Printf("Error writing to log: %v\n", err)
		return
	}

	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		resp, _ := ioutil.ReadAll(res.Body)
		fmt.Printf("Error writing to log: %v (%v)\n", string(resp), res.StatusCode)
	}
}
func (c *client) Flush() {
	c.queue.Flush()
}

func (c *client) Send(entries ...Entry) {
	for _, entry := range entries {
		c.queue.Add(entry)
	}
}

func (c *client) Close() {
	c.queue.Close()
}
