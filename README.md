# thrift_pool
golang版本的thrift的 pool，提供 pool、重试机制和取Client的超时。



## 安装

```golang
go get github.com/xuxiangyang/thrift_pool
```



## 例子

这个例子不能执行。可以参考



这个是rpc的一个封装，创建一个Pool

```golang
//rpc.go

package rpc

import (
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/cihub/seelog"
	"github.com/xuxiangyang/thrift_pool"
)

func NewPool(addrAndPort string) *thrift_pool.Pool {
	return thrift_pool.NewPool(addrAndPort, func(addr string) (transport thrift.TTransport, protocolFactory thrift.TProtocolFactory, err error) {
		var socket *thrift.TSocket
		socket, err = thrift.NewTSocket(addr)
		if err != nil {
			seelog.Error(err)
			return
		}

		transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
		transport = transportFactory.GetTransport(socket)
		err = transport.Open()
		if err != nil {
			seelog.Error(err)
			return
		}
		protocolFactory = thrift.NewTBinaryProtocolFactoryDefault()
		return
	})
}

```

下面是user client的使用



```golang
package user

import (
	"github.com/xuxiangyang/thrift_pool"
	"rpc"
	thrift_user "thrift/user"
	"time"
)

var Pool *thrift_pool.Pool

func Find(id string) (user *thrift_user.User, err error) {
	err = Pool.WithRetry(func(c *thrift_pool.Client) error {
		user, err = Client(c).Find(id)
		return err
	})
	return
}


func Client(client *thrift_pool.Client) *thrift_user.UserServiceClient {
	return thrift_user.NewUserServiceClientFactory(client.Transport, client.ProtocolFactory)
}

func init() {
	Pool = rpc.NewPool("127.0.0.1:8080")
	Pool.SetSize(8) //默认是8，可以不设置
	Pool.SetWaitTimeOut(5 * time.Sencond) //默认是5秒，可以不设置
}
```

其中Find是User的Service端提供的函数。user.thrift大概是这样的

```thrift
namespace go thrift.user

struct User {
  1: required string id
  2: required string name
  3: optional string signature
}

exception RecordNotFound {
  1: optional string message;
}

service UserService {
  User find(1: string id) throws (1: RecordNotFound not_found)
}
```





