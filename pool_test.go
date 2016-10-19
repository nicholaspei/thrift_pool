package thrift_pool

import (
	"errors"
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
		time.Sleep(time.Second)
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
		time.Sleep(100 * time.Millisecond)
		return &thrift.TFramedTransport{}, thrift.NewTBinaryProtocolFactoryDefault(), nil
	})

	for i := 0; i < DEFAULT_POOL_SIZE*10; i++ {
		err := pool.WithRetry(f)
		if err != nil {
			t.Log(i)
			t.Fatal(err)
		}
	}
}

func TestWithRery_GET_CLIENT_TIME_OUT(t *testing.T) {
	f := func(c *Client) error {
		time.Sleep(MAX_WAIT_TIME * 10)
		return nil
	}

	pool := NewPool("1", func(addrAndPort string) (thrift.TTransport, thrift.TProtocolFactory, error) {
		return &thrift.TFramedTransport{}, thrift.NewTBinaryProtocolFactoryDefault(), nil
	})
	pool.MaxWaitTime = 10 * time.Millisecond

	for len(pool.FreeClients) != DEFAULT_POOL_SIZE {
		time.Sleep(time.Millisecond)
	}

	wg := sync.WaitGroup{}
	wg.Add(DEFAULT_POOL_SIZE)
	for i := 0; i < DEFAULT_POOL_SIZE; i++ {
		go func(count int) {
			wg.Done()
			err := pool.WithRetry(f)
			if err != nil {
				t.Fatal(count)
				t.Fatal(err)
			}
		}(i)
	}
	wg.Wait()

	err := pool.WithRetry(f)
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

	for len(pool.FreeClients) != DEFAULT_POOL_SIZE {
		time.Sleep(time.Millisecond)
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
