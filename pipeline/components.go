package pipeline

import (
	"context"
	"sync"
)

const (
	CHANNEL_BUFFER_SIZE = 1
)

type Component interface {
	Run(*sync.WaitGroup, context.Context)
	Link(parent chan interface{})
	Subscribe() chan interface{}
	Close()
}

type Consumer struct {
	Consumes chan interface{}
}

func (c *Consumer) Link(parent chan interface{}) {
	c.Consumes = parent
}

type Publisher struct {
	sync.RWMutex
	subs   []chan interface{}
	closed bool
}

func (c *Publisher) Subscribe() chan interface{} {
	c.Lock()
	defer c.Unlock()

	if c.closed {
		return nil
	}

	ch := make(chan interface{}, CHANNEL_BUFFER_SIZE)
	c.subs = append(c.subs, ch)
	return ch
}

func (c *Publisher) Publish(msg interface{}, sendToAll bool) {
	c.RLock()
	defer c.RUnlock()

	if c.closed {
		return
	}

	for _, ch := range c.subs {
		ch <- msg
		if !sendToAll { // if we are only sending to the first accepting consumer
			ok := <-ch // wait for channel answer
			if ok.(bool) {
				return
			}
		}
	}
}

func (c *Publisher) Close() {
	c.Lock()
	defer c.Unlock()

	if !c.closed {
		c.closed = true
		for _, ch := range c.subs {
			close(ch)
		}
	}
}
