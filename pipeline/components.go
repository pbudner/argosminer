package pipeline

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/mitchellh/mapstructure"
	"go.uber.org/zap"
)

var (
	registered_components = make(map[string]RegisteredComponent)
)

type RegisteredComponent struct {
	Config          interface{}
	InitializerFunc func(interface{}) Component
}

type Component interface {
	Run(*sync.WaitGroup, context.Context)
	Link(parent chan interface{})
	Subscribe() chan interface{}
	Close()
}

func RegisterComponent(name string, config interface{}, initializerFunc func(interface{}) Component) {
	registered_components[name] = RegisteredComponent{
		Config:          config,
		InitializerFunc: initializerFunc,
	}
}

func InstantiateComponent(name string, args map[string]interface{}) (Component, error) {
	comp, found := registered_components[name]
	if !found {
		return nil, errors.New("pipeline component not defined")
	}

	dec, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		TagName:          "yaml",
		DecodeHook:       mapstructure.StringToTimeDurationHookFunc(),
		WeaklyTypedInput: true,
		Result:           &comp.Config,
	})

	if err != nil {
		return nil, err
	}

	if err = dec.Decode(args); err != nil {
		return nil, err
	}

	return comp.InitializerFunc(comp.Config), nil
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

	ch := make(chan interface{})
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
		select {
		case rOk := <-ch: // wait for channel answer
			if !sendToAll { // if we are only sending to the first accepting consumer
				ok, parsed := rOk.(bool)
				if !parsed {
					zap.L().Sugar().With("service", "publisher").Error("Expected bool, but received something different")
					continue
				}
				if ok {
					return
				}
			}
		case <-time.After(1 * time.Second): // timeout
			zap.L().Sugar().With("service", "publisher").Info("Subscribed component did not answer timely")
			continue
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
