package etcd

import (
	log "github.com/alecthomas/log4go"
	"golang.org/x/net/context"
	"strings"
	"sync"
	"time"
)

type Exchange struct {
	From        string
	To          string
	WorkerGroup string
	OpEvent     OperationEvent
}

//Etcd服务注册
type EtcdRegistry struct {
	lock            sync.RWMutex
	registryClient  *Client
	registryContext context.Context
	workers         map[string]*AgentWorker
	exchangeChan    chan *Exchange
	isClosed        bool
}

func NewEtcdRegistry(hosts string) *EtcdRegistry {
	log.Info("etcd host info: %s \n", hosts)
	etcdAddress := strings.Split(hosts, ",")
	Init(etcdAddress)
	cli := GetClient()
	ctx := GetContext()

	return &EtcdRegistry{
		registryClient:  cli,
		registryContext: ctx,
		workers:         make(map[string]*AgentWorker, 5),
		exchangeChan:    make(chan *Exchange, 4096),
		isClosed:        false}
}

//启动服务注册中心
func (self *EtcdRegistry) Start() {
	//同步心跳
	go self.heartbeat()

	//检查故障
	go self.checkAlive()

	//服务发现
	go self.discovery()

	//工作调度
	go self.schedule()
}

//服务心跳
func (self *EtcdRegistry) heartbeat() {
	for !self.isClosed {
		err := self.registryClient.client.AutoSync(context.Background(), 10*time.Second)
		if err == context.DeadlineExceeded || err == context.Canceled {
			break
		}
	}
}

//负责检查agent的存活性
func (self *EtcdRegistry) checkAlive() {
	for !self.isClosed {
		for _, w := range self.workers {
			go w.Keepalive()
		}
		time.Sleep(CHECK_ALIVE_INTERVAL)
	}
}

//服务发现
func (self *EtcdRegistry) discovery() {
	log.Info("service monitor begin")
	nodeinfo, err := self.registryClient.GetDirChildren(DISCOVERY)
	if err != nil {
		log.Error("error to get nodes from %s", DISCOVERY)
	}

	//首次发现系统节点
	for _, group := range nodeinfo {
		membersDir := group + "/members"
		memmbers, err := self.registryClient.GetDirChildren(membersDir)
		if err != nil {
			log.Error("error to get nodes from %s", membersDir)
		}

		//获取组的成员信息
		for _, agent := range memmbers {
			log.Info("register a agent [%v] to a worker", agent)
			if _, ok := self.workers[group]; !ok {
				self.registWorker(group, agent)
			}
		}
	}

	//监听组目录
	worker, err := self.registryClient.CreateDirWatcher(DISCOVERY)
	if err != nil {
		return
	}
	go func() {
		for !self.isClosed {
			resp, err := worker.Next(ctx)
			if err != nil {
				continue
			}
			switch resp.Action {
			case "create", "update": //新增,修改
				go self.handleCreateEvent(resp.Node.Key)
			case "delete": //过期,删除
				go self.handleRemoveEvent(resp.Node.Key)
			default:
			}
		}
	}()
}

//工作调度
func (self *EtcdRegistry) schedule() {
	for !self.isClosed {
		log.Info("worker request agent exchange")
		select {
		case ex := <-self.exchangeChan:
			go self.handleExchange(ex)
		}
	}
}

//根据完整路径获取组与节点名称
func (self *EtcdRegistry) getGroupAndAgentFromFullPath(dir string) (string, string) {
	// ===> /apps/agent-groups/devops-001/members/localhost/heartbeat
	if strings.Contains(dir, "/abus/agent-groups/") &&
		strings.Contains(dir, "/members/") && strings.Contains(dir, "/heartbeat") {
		newGroupName := dir[:strings.Index(dir, "/members/")]
		newAgentName := dir[:strings.Index(dir, "/heartbeat")]
		return newGroupName, newAgentName
	}
	return "", ""
}

//agent注册处理
func (self *EtcdRegistry) handleCreateEvent(dir string) {
	group, agent := self.getGroupAndAgentFromFullPath(dir)

	log.Info("%s ==== %s", group, agent)

	if group != "" && agent != "" {
		self.registWorker(group, agent)
	}
}

//元素移除处理
func (self *EtcdRegistry) handleRemoveEvent(dir string) {
	log.Info("[DELETE] >> %s", dir)
}

//注册keepalive worker
func (self *EtcdRegistry) registWorker(group, agent string) {
	if _, ok := self.workers[group]; !ok {
		log.Info("[ADD] new group >> %s", group)
		log.Info("[ADD] new agent >> %s", agent)
		w := NewWorker(self, 1*time.Second, group, agent)
		self.lock.RLock()
		self.workers[group] = w
		self.lock.RUnlock()
	}
}

//注销 worker
func (self *EtcdRegistry) unRegistWorker(group string) {
	if w, ok := self.workers[group]; ok {
		w.StopWorking()
		log.Info("[REMOVE] old group >> %s", group)
		self.lock.RLock()
		delete(self.workers, group)
		self.lock.RUnlock()

	}
}

func (self *EtcdRegistry) GetWorkers() map[string]*AgentWorker {
	return self.workers
}

func (self *EtcdRegistry) handleExchange(ex *Exchange) {
	switch ex.OpEvent {
	case UpdateEvent:
		println("I received a event UPDATE")
	case ExitEvent:
		log.Info("handle a exit event from group : %s", ex.WorkerGroup)
		self.unRegistWorker(ex.WorkerGroup)
	case StopEvent:
		log.Info("handle a stop event from group : %s", ex.WorkerGroup)
	default:
		goto finish
	}
finish:
	log.Info("handle exchange finish")
}

//注册服务关闭
func (self *EtcdRegistry) Close() {
	self.isClosed = true
}
