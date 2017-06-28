package app

import (
	"fmt"
	"github.com/domac/hasky/etcd"
	"log"
	"net"
	"os"
	"sync"
)

type Appd struct {
	sync.RWMutex
	opts *Options

	httpListener net.Listener
	waitGroup    WaitGroupWrapper

	exitChan chan int
	isExit   bool

	etcdRegistry *etcd.EtcdRegistry
}

func New(opts *Options) *Appd {
	app := &Appd{
		opts:     opts,
		exitChan: make(chan int),
	}
	log.Println(VerString())
	return app
}

func (self *Appd) SetEtcdRegistry(registry *etcd.EtcdRegistry) {
	self.etcdRegistry = registry
}

func (self *Appd) logf(f string, args ...interface{}) {
	if self.opts.Logger == nil {
		return
	}
	self.opts.Logger.Output(2, fmt.Sprintf(f, args...))
}

//后台运行入口
func (self *Appd) Main() {
	ctx := &context{appd: self}
	httpListener, err := net.Listen("tcp", self.opts.HTTPAddress)
	if err != nil {
		self.logf("FATAL: listen (%s) failed - %s", self.opts.HTTPAddress, err)
		os.Exit(1)
	}
	self.Lock()
	self.httpListener = httpListener
	self.Unlock()
	httpServer := newHTTPServer(ctx)
	//开启对外提供的http服务
	self.waitGroup.Wrap(func() {
		Serve(self.httpListener, httpServer, "HTTP", self.opts.Logger)
	})

	registry := etcd.NewEtcdRegistry(self.opts.EtcdEndpoint)
	self.SetEtcdRegistry(registry)

	//启动Etcd服务发现
	self.waitGroup.Wrap(func() { self.EtcdLookup() })
}

func (self *Appd) Exit() {
	if self.httpListener != nil {
		self.httpListener.Close()
	}

	if self.etcdRegistry != nil {
		self.etcdRegistry.Close()
	}
	close(self.exitChan)
	self.isExit = true
	self.waitGroup.Wait()
}
