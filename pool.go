package thrift_pool

import (
	"errors"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/cihub/seelog"
	"github.com/satori/go.uuid"
	"sync"
	"time"
)

const (
	DEFAULT_POOL_SIZE = 8
	MAX_TRY_TIMES     = 3
	MAX_WAIT_TIME     = 5 * time.Second
	SLEEP_INTERVAL    = 10 * time.Millisecond
)

type Client struct {
	UUID            string
	Transport       thrift.TTransport
	ProtocolFactory thrift.TProtocolFactory
}

type Pool struct {
	Lock         *sync.Mutex
	Size         int
	WaitTimeOut  time.Duration
	AddrAndPort  string
	Block        func(addrAndPort string) (*Client, error)
	FreeClients  map[string]*Client
	UsingClients map[string]*Client
}

func NewPool(addrAndPort string, f func(addrAndPort string) (t thrift.TTransport, p thrift.TProtocolFactory, err error)) *Pool {
	block := func(addrAndPort string) (*Client, error) {
		t, p, err := f(addrAndPort)
		u := uuid.NewV4().String()
		client := &Client{UUID: u, Transport: t, ProtocolFactory: p}
		return client, err
	}
	return &Pool{AddrAndPort: addrAndPort, Block: block, FreeClients: map[string]*Client{}, UsingClients: map[string]*Client{}, Size: DEFAULT_POOL_SIZE, Lock: &sync.Mutex{}, WaitTimeOut: MAX_WAIT_TIME}
}

func (this *Pool) SetSize(size int) {
	this.Size = size
}

func (this *Pool) SetWaitTimeOut(t time.Duration) {
	this.WaitTimeOut = t
}

func (this *Pool) Get() (client *Client, err error) {
	if len(this.FreeClients)+len(this.UsingClients) < this.Size {
		client, err = this.Block(this.AddrAndPort)
		if err == nil {
			this.MutexAddUsingClients(client)
		}
		return
	}

	sleepedTime := 0 * time.Millisecond
	for {
		if len(this.FreeClients) > 0 {
			break
		}
		time.Sleep(SLEEP_INTERVAL)
		sleepedTime += SLEEP_INTERVAL
		if sleepedTime > this.WaitTimeOut {
			err = TimeOut{"timeout to get a client"}
			return
		}
	}
	client, err = this.MutexMoveFreeToUsing()
	return
}

func (this *Pool) MutexAddUsingClients(client *Client) {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	this.UsingClients[client.UUID] = client
}

func (this *Pool) MutexMoveFreeToUsing() (client *Client, err error) {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	for _, v := range this.FreeClients {
		client = v
		break
	}

	if client == nil {
		return nil, errors.New("can not get client")
	}

	delete(this.FreeClients, client.UUID)
	this.UsingClients[client.UUID] = client
	return
}

func (this *Pool) PutBack(client *Client) {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	delete(this.UsingClients, client.UUID)
	this.FreeClients[client.UUID] = client
}

func (this *Pool) Remove(client *Client) {
	this.Lock.Lock()
	defer this.Lock.Unlock()
	delete(this.UsingClients, client.UUID)
	delete(this.FreeClients, client.UUID)
}

func (this *Pool) WithRetry(closure func(client *Client) error) error {
	for i := 0; i < MAX_TRY_TIMES; i++ {
		client, err := this.Get()
		if err != nil {
			seelog.Error(err)
			return err
		}

		err = closure(client)

		if err == nil {
			this.PutBack(client)
			return nil
		} else {
			this.Remove(client)
			_, ok := err.(thrift.TTransportException)
			if ok {
				continue
			} else {
				return err
			}
		}
	}

	err := errors.New("thrift pool closure run error")
	seelog.Error(err)
	return err
}
