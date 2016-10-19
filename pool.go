package thrift_pool

import (
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/cihub/seelog"
	"sync"
	"time"
)

const (
	DEFAULT_POOL_SIZE = 1024
	PREHEAT_COUNT     = 128
	MAX_WAIT_TIME     = 5 * time.Second
)

var (
	ErrorLogFunc func(e error)
)

type Client struct {
	Transport       thrift.TTransport
	ProtocolFactory thrift.TProtocolFactory
}

type Pool struct {
	AddrAndPort      string
	FreeClients      chan *Client
	NewTransportFunc func(addrAndPort string) (t thrift.TTransport, p thrift.TProtocolFactory, err error)
	MaxWaitTime      time.Duration
	activeCount      int
	MaxSize          int
	mu               sync.Mutex
	activeCountMu    sync.Mutex
}

func NewPool(addrAndPort string, f func(addrAndPort string) (t thrift.TTransport, p thrift.TProtocolFactory, err error)) *Pool {
	this := &Pool{
		AddrAndPort:      addrAndPort,
		FreeClients:      make(chan *Client, DEFAULT_POOL_SIZE),
		NewTransportFunc: f,
		MaxWaitTime:      MAX_WAIT_TIME,
		activeCount:      0,
		mu:               sync.Mutex{},
		activeCountMu:    sync.Mutex{},
		MaxSize:          DEFAULT_POOL_SIZE,
	}

	for i := 0; i < PREHEAT_COUNT; i++ {
		go this.AddClient()
	}
	return this
}

func NewClient(addrAndPort string, f func(addrAndPort string) (t thrift.TTransport, p thrift.TProtocolFactory, err error)) (*Client, error) {
	t, p, err := f(addrAndPort)
	client := &Client{Transport: t, ProtocolFactory: p}
	return client, err
}

func (this *Pool) SetMaxSize(i int) {
	this.MaxSize = i
}

func (this *Pool) AddClient() {
	client, err := NewClient(this.AddrAndPort, this.NewTransportFunc)
	if err != nil {
		ErrorLogFunc(err)
		return
	}
	this.FreeClients <- client
}

func (this *Pool) Get() (*Client, error) {
	defer this.ActiveCountPlus(1)
	for i := 0 * time.Second; i < this.MaxWaitTime; i += 100 * time.Millisecond {
		select {
		case <-time.After(100 * time.Millisecond):
			this.mu.Lock()
			if this.ActiveCount()+len(this.FreeClients) < this.MaxSize {
				return NewClient(this.AddrAndPort, this.NewTransportFunc)
			}
			this.mu.Unlock()
		case c := <-this.FreeClients:
			return c, nil
		}
	}
	return nil, TimeOut{"time out to get client"}
}

func (this *Pool) PutBack(c *Client) {
	defer this.ActiveCountPlus(-1)
	this.FreeClients <- c
}

func (this *Pool) Remove(c *Client) {
	defer this.ActiveCountPlus(-1)
}

func (this *Pool) WithRetry(closure func(client *Client) error) error {
	var err error
	var client *Client
	for i := 0; i < this.MaxSize+1; i++ {
		client, err = this.Get()
		if err != nil {
			ErrorLogFunc(err)
			return err
		}

		err = closure(client)

		if err == nil {
			go this.PutBack(client)
			return nil
		} else {
			_, ok := err.(thrift.TTransportException)
			if ok {
				go this.Remove(client)
				continue
			} else {
				go this.PutBack(client)
				return err
			}
		}
	}

	ErrorLogFunc(err)
	return err
}

func (this *Pool) ActiveCount() int {
	this.activeCountMu.Lock()
	defer this.activeCountMu.Unlock()
	return this.activeCount
}

func (this *Pool) ActiveCountPlus(i int) {
	this.activeCountMu.Lock()
	defer this.activeCountMu.Unlock()
	this.activeCount += i
}

func init() {
	ErrorLogFunc = func(e error) {
		seelog.Error(e)
	}
}
