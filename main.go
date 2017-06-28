package main

import (
	"flag"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/domac/hasky/app"
	"github.com/judwhite/go-svc/svc"
	"github.com/mreiferson/go-options"
	"log"
	"os"
	"path/filepath"
	"syscall"
)

var (
	flagSet      = flag.NewFlagSet("hasky", flag.ExitOnError)
	showVersion  = flagSet.Bool("version", false, "print version string") //版本
	config       = flagSet.String("config", "", "path to config file")
	httpAddress  = flagSet.String("http-address", "0.0.0.0:16630", "<addr>:<port> to listen on for HTTP clients")
	etcdEndpoint = flagSet.String("etcd-endpoint", "0.0.0.0:2379", "ectd service discovery address")
)

//程序封装
type program struct {
	appd *app.Appd
}

func (p *program) Init(env svc.Environment) error {
	if env.IsWindowsService() {
		//切换工作目录
		dir := filepath.Dir(os.Args[0])
		return os.Chdir(dir)
	}
	return nil
}

//程序启动
func (p *program) Start() error {
	flagSet.Parse(os.Args[1:])
	if *showVersion {
		fmt.Println(app.VerString())
		os.Exit(0)
	}

	var cfg map[string]interface{}
	if *config != "" {
		_, err := toml.DecodeFile(*config, &cfg)
		if err != nil {
			log.Fatalf("ERROR: failed to load config file %s - %s", *config, err.Error())
		}
	}

	opts := app.NewOptions()
	options.Resolve(opts, flagSet, cfg)

	//后台进程创建
	daemon := app.New(opts)
	daemon.Main()
	p.appd = daemon
	return nil
}

//程序停止
func (p *program) Stop() error {
	if p.appd != nil {
		p.appd.Exit()
	}
	return nil
}

//引导程序
func main() {
	prg := &program{}
	if err := svc.Run(prg, syscall.SIGINT, syscall.SIGTERM); err != nil {
		log.Fatal(err)
	}
}
