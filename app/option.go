package app

import (
	"log"
	"os"
)

//配置选项
type Options struct {
	HTTPAddress  string `flag:"http-address"`
	EtcdEndpoint string `flag:"etcd-endpoint"`
	Logger       Logger
}

func NewOptions() *Options {
	return &Options{
		HTTPAddress:  "0.0.0.0:13360",
		EtcdEndpoint: "0.0.0.0:2379",
		Logger:       log.New(os.Stderr, "[HUSKY] ", log.Ldate|log.Ltime|log.Lmicroseconds),
	}
}
