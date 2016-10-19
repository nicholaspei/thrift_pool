package thrift_pool

import (
	"errors"
	_ "fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"sync"
	"testing"
	"time"
)

func TestWithRery_OK(t *testing.T) {
	f := func(c *Client) error {
		return nil
	}

	pool := NewPool("1", func(addrAndPort string) (thrift.TTransport, thrift.TProtocolFactory, error) {
		return &thrift.TFramedTransport{}, thrift.NewTBinaryProtocolFactoryDefault(), nil
	})

	err := pool.WithRetry(f)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWithRery_INSTABILITY(t *testing.T) {
	count := 0
	f := func(c *Client) error {
		count += 1
		if count%3 == 0 {
			return thrift.NewTTransportException(thrift.TIMED_OUT, "err")
		} else {
			return nil
		}
	}

	pool := NewPool("1", func(addrAndPort string) (thrift.TTransport, thrift.TProtocolFactory, error) {
		return &thrift.TFramedTransport{}, thrift.NewTBinaryProtocolFactoryDefault(), nil
	})
	pool.SetMaxSize(32)

	for i := 0; i < pool.MaxSize; i++ {
		err := pool.WithRetry(f)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestWithRery_GET_CLIENT_TIME_OUT(t *testing.T) {
	pool := NewPool("1", func(addrAndPort string) (thrift.TTransport, thrift.TProtocolFactory, error) {
		return &thrift.TFramedTransport{}, thrift.NewTBinaryProtocolFactoryDefault(), nil
	})
	pool.MaxWaitTime = 10 * time.Millisecond
	pool.SetMaxSize(32)

	for i := len(pool.FreeClients); i < pool.MaxSize; i++ {
		pool.Get()
	}

	_, err := pool.Get()
	if err == nil {
		t.Fatal("should get error")
	}
}

func TestWithRery_ERROR(t *testing.T) {
	e := errors.New("e")
	f := func(c *Client) error {
		return e
	}

	pool := NewPool("1", func(addrAndPort string) (thrift.TTransport, thrift.TProtocolFactory, error) {
		return &thrift.TFramedTransport{}, thrift.NewTBinaryProtocolFactoryDefault(), nil
	})

	err := pool.WithRetry(f)
	if err != e {
		t.Fatal("should get", e)
	}
}

func TestWithRery_TTRANSPORTEXCEPTION(t *testing.T) {
	e := thrift.NewTTransportException(thrift.TIMED_OUT, "err")
	f := func(c *Client) error {
		return e
	}

	pool := NewPool("1", func(addrAndPort string) (thrift.TTransport, thrift.TProtocolFactory, error) {
		return &thrift.TFramedTransport{}, thrift.NewTBinaryProtocolFactoryDefault(), nil
	})
	pool.SetMaxSize(8)

	err := pool.WithRetry(f)
	if err != e {
		t.Fatal("should get", e)
	}
}

func TestWithRery_NEW_CLIENT_TTRANSPORTEXCEPTION(t *testing.T) {
	e := thrift.NewTTransportException(thrift.TIMED_OUT, "err")
	ErrorLogFunc = func(err error) {
		if err != e {
			t.Fatal("should get", e)
		}
	}

	pool := NewPool("1", func(addrAndPort string) (thrift.TTransport, thrift.TProtocolFactory, error) {
		return &thrift.TFramedTransport{}, thrift.NewTBinaryProtocolFactoryDefault(), e
	})

	pool.Get()
}

func TestWithRery_CLIENT_RESTART(t *testing.T) {
	f := func(c *Client) error {
		c.Alive = false
		return nil
	}
	pool := NewPool("1", func(addrAndPort string) (thrift.TTransport, thrift.TProtocolFactory, error) {
		return &thrift.TFramedTransport{}, thrift.NewTBinaryProtocolFactoryDefault(), nil
	})
	pool.SetMaxSize(100)
	for i := 0; i < 100; i++ {
		pool.WithRetry(f)
	}

	err := pool.WithRetry(func(c *Client) error {
		if c.Alive {
			return nil
		} else {
			return thrift.NewTTransportException(thrift.TIMED_OUT, "err")
		}
	})

	if err != nil {
		t.Fatal(err)
	}
}

func BenchmarkWithRetry(b *testing.B) {
	f := func(c *Client) error {
		time.Sleep(100 * time.Millisecond)
		return nil
	}

	pool := NewPool("1", func(addrAndPort string) (thrift.TTransport, thrift.TProtocolFactory, error) {
		time.Sleep(200 * time.Millisecond)
		return &thrift.TFramedTransport{}, thrift.NewTBinaryProtocolFactoryDefault(), nil
	})

	wg := sync.WaitGroup{}
	wg.Add(b.N)
	for i := 0; i < b.N; i++ {
		go func() {
			defer wg.Done()
			err := pool.WithRetry(f)
			if err != nil {
				b.Fatal(err)
			}
		}()
	}
	wg.Wait()
}
