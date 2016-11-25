package app

import (
	log "github.com/alecthomas/log4go"
)

//Etcd服务发现
func (app *Appd) EtcdLookup() {
	log.Info("etcd lookup")
	app.etcdRegistry.Start()
}
