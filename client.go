package elastilog

import (
	"bytes"
	"encoding/json"
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

type elasticStatus struct {
	Took   int  `json:"took"`
	Errors bool `json:"errors"`
}

func (c *client) writemsgs(msgs []Entry) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("Panic writing log: %s", err)
		}
	}()
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
		fmt.Printf("Error writing to log: Status %v\n", res.StatusCode)
		return
	}

	resp, _ := ioutil.ReadAll(res.Body)
	var status elasticStatus
	err = json.Unmarshal(resp, &status)
	if err != nil {
		fmt.Printf("Error writing to log: %v (%v)\n", string(resp), err)
		return
	}
	if status.Errors {
		fmt.Printf("Error writing to log: %v\n", string(resp))
		return
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
