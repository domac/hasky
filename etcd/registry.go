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
	workers         map[string]*LeaderWorker
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
		workers:         make(map[string]*LeaderWorker, 5),
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
		if err != nil || memmbers == nil {
			log.Error("error to get members from %s", membersDir)
			continue
		}

		log.Info("register a group [%s] to a worker", group)

		if _, ok := self.workers[group]; !ok {
			self.registWorker(group)
		}
	}

	//监听组目录
	discoverWatcher, err := self.registryClient.CreateDirWatcher(DISCOVERY)
	if err != nil {
		return
	}
	go func() {
		for !self.isClosed {
			resp, err := discoverWatcher.Next(ctx)
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
		select {
		case ex := <-self.exchangeChan:
			go self.handleExchange(ex)
		}
	}
}

//根据完整路径获取组与节点名称
func (self *EtcdRegistry) getGroupAndAgentFromFullPath(dir string) (string, string) {
	// ===> /hasky/agent-groups/devops-001/members/localhost/heartbeat
	if strings.Contains(dir, "/hasky/agent-groups/") &&
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
	if group != "" && agent != "" {
		self.registWorker(group)
	}
}

//元素移除处理
func (self *EtcdRegistry) handleRemoveEvent(dir string) {
	log.Info("[DELETE] >> %s", dir)

	if g, ok := self.workers[dir]; ok {
		log.Info("[DELETE][GROUP] >> %s", g.Group)
		self.unRegistWorker(dir)
	}
}

//注册keepalive worker
func (self *EtcdRegistry) registWorker(group string) {
	if _, ok := self.workers[group]; !ok {
		log.Info("[ADD] REGISTER GROUP WORKER : %s ", group)
		w := NewLeaderWorker(self, 1*time.Second, group)
		self.lock.RLock()
		self.workers[group] = w
		self.lock.RUnlock()
		w.StartWorking()
	}
}

//注销 worker
func (self *EtcdRegistry) unRegistWorker(group string) {
	if w, ok := self.workers[group]; ok {
		w.StopWorking()
		log.Info("[REMOVE] UN-REGISTER GROUP >> %s", group)
		self.lock.RLock()
		delete(self.workers, group)
		self.lock.RUnlock()

	}
}

func (self *EtcdRegistry) GetWorkers() map[string]*LeaderWorker {
	return self.workers
}

func (self *EtcdRegistry) updateGroupLeader(group string, oldNode, newNode string) {
	if oldNode != newNode {
		self.SetGroupLeader(group, newNode)

		w := self.workers[group]
		w.WorkingNode = newNode
	}
}

func (self *EtcdRegistry) handleExchange(ex *Exchange) {
	switch ex.OpEvent {
	case UpdateEvent:
		self.updateGroupLeader(ex.WorkerGroup, ex.From, ex.To)
	case ExitEvent:
		self.unRegistWorker(ex.WorkerGroup)
	case StopEvent:
		self.StopLeaderRunning(ex.WorkerGroup)
	}
}

//停止当前的leader运行
func (self *EtcdRegistry) StopLeaderRunning(group string) {
	leader := self.GetGroupLeader(group)
	log.Info("STOP LEADER RUNNING : %s", leader)
}

//获取组leader名称
func (self *EtcdRegistry) GetGroupLeader(group string) string {
	leaderFile := group + "/leader"
	leader, err := self.registryClient.Get(leaderFile)
	if err != nil {
		return ""
	}
	return leader
}

func (self *EtcdRegistry) SetGroupLeader(group string, leader string) error {
	leaderFile := group + "/leader"
	return self.registryClient.Set(leaderFile, leader)
}

//注册服务关闭
func (self *EtcdRegistry) Close() {
	self.isClosed = true
}
