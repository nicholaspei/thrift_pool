package thrift_pool

import (
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/cihub/seelog"
	"time"
)

const (
	DEFAULT_POOL_SIZE = 256
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
}

func NewPool(addrAndPort string, f func(addrAndPort string) (t thrift.TTransport, p thrift.TProtocolFactory, err error)) *Pool {
	this := &Pool{
		AddrAndPort:      addrAndPort,
		FreeClients:      make(chan *Client, DEFAULT_POOL_SIZE),
		NewTransportFunc: f,
		MaxWaitTime:      MAX_WAIT_TIME,
	}

	for i := 0; i < DEFAULT_POOL_SIZE; i++ {
		go this.AddClient()
	}
	return this
}

func NewClient(addrAndPort string, f func(addrAndPort string) (t thrift.TTransport, p thrift.TProtocolFactory, err error)) (*Client, error) {
	t, p, err := f(addrAndPort)
	client := &Client{Transport: t, ProtocolFactory: p}
	return client, err
}

func (this *Pool) AddClient() {
	client, err := NewClient(this.AddrAndPort, this.NewTransportFunc)
	if err != nil {
		ErrorLogFunc(err)
	}
	this.FreeClients <- client
}

func (this *Pool) Get() (*Client, error) {
	select {
	case <-time.After(this.MaxWaitTime):
		return nil, TimeOut{"timeout to get a client"}
	case c := <-this.FreeClients:
		return c, nil
	}
}

func (this *Pool) PutBack(c *Client) {
	this.FreeClients <- c
}

func (this *Pool) WithRetry(closure func(client *Client) error) error {
	var err error
	var client *Client
	for i := 0; i < DEFAULT_POOL_SIZE+1; i++ {
		client, err = this.Get()
		if err != nil {
			ErrorLogFunc(err)
			return err
		}

		err = closure(client)

		if err == nil {
			this.PutBack(client)
			return nil
		} else {
			go this.AddClient()
			_, ok := err.(thrift.TTransportException)
			if ok {
				continue
			} else {
				return err
			}
		}
	}

	ErrorLogFunc(err)
	return err
}

func init() {
	ErrorLogFunc = func(e error) {
		seelog.Error(e)
	}
}
