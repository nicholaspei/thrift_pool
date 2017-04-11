package thrift_pool

import (
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/cihub/seelog"
	"sync"
	"time"
)

const (
	DEFAULT_POOL_SIZE = 128
	MAX_WAIT_TIME     = 5 * time.Second
)

var (
	ErrorLogFunc func(e error)
)

type Client struct {
	Transport       thrift.TTransport
	ProtocolFactory thrift.TProtocolFactory
	Alive           bool
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

	return this
}

func NewClient(addrAndPort string, f func(addrAndPort string) (t thrift.TTransport, p thrift.TProtocolFactory, err error)) (*Client, error) {
	t, p, err := f(addrAndPort)
	client := &Client{Transport: t, ProtocolFactory: p, Alive: true}
	return client, err
}

func (this *Pool) SetMaxSize(i int) {
	this.MaxSize = i
	this.FreeClients = make(chan *Client, i)
}

func (this *Pool) Get() (*Client, error) {
	defer this.ActiveCountPlus(1)
	this.mu.Lock()
	if this.ActiveCount()+len(this.FreeClients) < this.MaxSize {
		defer this.mu.Unlock()
		return NewClient(this.AddrAndPort, this.NewTransportFunc)
	} else {
		this.mu.Unlock()
	}

	select {
	case <-time.After(this.MaxWaitTime):
		return nil, TimeOut{"time out to get client"}
	case c := <-this.FreeClients:
		return c, nil
	}
}

func (this *Pool) PutBack(c *Client) {
	defer this.ActiveCountPlus(-1)
	this.FreeClients <- c
}

func (this *Pool) Remove(c *Client) {
	defer this.ActiveCountPlus(-1)
	c.Transport.Close()
}

func (this *Pool) WithRetry(closure func(client *Client) error) error {
	client, err := this.Get()
	if err != nil {
		return err
	}

	err = closure(client)
	if err == nil {
		this.PutBack(client)
		return nil
	} else {
		_, ok := err.(thrift.TTransportException)
		if ok {
			this.Remove(client)
			for i := 0; i < 3; i++ {
				client, err = NewClient(this.AddrAndPort, this.NewTransportFunc)
				if err == nil {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			if err != nil {
				return err
			}
			err = closure(client)
			client.Transport.Close()
			return err
		} else {
			this.PutBack(client)
			return err
		}
	}
	return nil
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
