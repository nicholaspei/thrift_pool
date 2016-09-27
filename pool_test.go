package thrift_pool

import (
	"errors"
	"git.apache.org/thrift.git/lib/go/thrift"
	"testing"
	"time"
)

func Test_GetWithBlank(t *testing.T) {
	pool := NewPool("1", func(addrAndPort string) (t thrift.TTransport, p thrift.TProtocolFactory, err error) {
		return &thrift.TFramedTransport{}, thrift.NewTBinaryProtocolFactoryDefault(), nil
	})

	pool.SetWaitTimeOut(21 * time.Millisecond)
	pool.SetSize(1)
	_, err := pool.Get()
	if err != nil {
		t.Fatal("should not get error")
	}
	_, err = pool.Get()
	if err == nil {
		t.Fatal("should get error")
	}
	_, ok := err.(TimeOut)
	if !ok {
		t.Fatal("should get timeout error")
	}
}

func Test_PutBackAndRemove(t *testing.T) {
	pool := NewPool("1", func(addrAndPort string) (t thrift.TTransport, p thrift.TProtocolFactory, err error) {
		return &thrift.TFramedTransport{}, thrift.NewTBinaryProtocolFactoryDefault(), nil
	})

	if len(pool.FreeClients) != 0 {
		t.Fatal("FreeClients Size should be 0")
	}
	if len(pool.UsingClients) != 0 {
		t.Fatal("UsingClients Size should be 0")
	}

	client, err := pool.Get()
	if err != nil {
		t.Fatal(err)
	}

	if len(pool.FreeClients) != 0 {
		t.Fatal()
	}
	if len(pool.UsingClients) != 1 {
		t.Fatal()
	}

	pool.PutBack(client)
	if len(pool.FreeClients) != 1 {
		t.Fatal()
	}
	if len(pool.UsingClients) != 0 {
		t.Fatal()
	}

	pool.Remove(client)
	if len(pool.FreeClients) != 0 {
		t.Fatal("FreeClients Size should be 0")
	}
	if len(pool.UsingClients) != 0 {
		t.Fatal("UsingClients Size should be 0")
	}

}

func Test_WithRetry(t *testing.T) {
	pool := NewPool("1", func(addrAndPort string) (t thrift.TTransport, p thrift.TProtocolFactory, err error) {
		return &thrift.TFramedTransport{}, thrift.NewTBinaryProtocolFactoryDefault(), nil
	})

	i := 0
	pool.WithRetry(func(client *Client) error {
		i = 1
		return nil
	})

	if len(pool.FreeClients) != 1 {
		t.Fatal()
	}

	if len(pool.UsingClients) != 0 {
		t.Fatal()
	}

	if i != 1 {
		t.Fatal()
	}
}

func Test_WithRetryInstability(t *testing.T) {
	pool := NewPool("1", func(addrAndPort string) (t thrift.TTransport, p thrift.TProtocolFactory, err error) {
		return &thrift.TFramedTransport{}, thrift.NewTBinaryProtocolFactoryDefault(), nil
	})
	i := 0
	pool.WithRetry(func(client *Client) error {
		i += 1
		if i == 1 {
			return thrift.NewTTransportException(thrift.TIMED_OUT, "hi")
		} else {
			return nil
		}
	})

	if i != 2 {
		t.Fatal()
	}
}

func Test_WithRetryOtherException(t *testing.T) {
	pool := NewPool("1", func(addrAndPort string) (t thrift.TTransport, p thrift.TProtocolFactory, err error) {
		return &thrift.TFramedTransport{}, thrift.NewTBinaryProtocolFactoryDefault(), nil
	})
	i := 0
	pool.WithRetry(func(client *Client) error {
		i += 1
		return errors.New("shji")
	})

	if i != 1 {
		t.Fatal()
	}
}
