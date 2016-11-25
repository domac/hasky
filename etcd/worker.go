package etcd

import (
	"errors"
	"fmt"
	log "github.com/alecthomas/log4go"
	"strconv"
	"strings"
	"time"
)

//负责每个组的leader agent的alive检测
type AgentWorker struct {
	registry        *EtcdRegistry
	Group           string
	WorkingNode     string
	LastWorkingNode string
	KeepalivePeriod time.Duration
	LastKeepalive   time.Time
}

//创建判官
func NewWorker(reg *EtcdRegistry, period time.Duration, group string, agent string) *AgentWorker {
	return &AgentWorker{
		registry:        reg,
		KeepalivePeriod: period,
		Group:           group,
		WorkingNode:     agent}
}

func (self *AgentWorker) StopWorking() {
	self.WorkingNode = ""
}

func (self *AgentWorker) StartWorking(workingNode string) {
	self.WorkingNode = workingNode
}

//需要做得工作：
//1. 检查所属的组是否存在,如果不存在,则告诉注册器,把自己干掉
//2. 检查监控的agent是否还存在
//3. 检查监控的agent心跳
func (self *AgentWorker) Keepalive() {
	if self.WorkingNode == "" {
		log.Info("worker is waiting for a working node")
		return
	}

	hbdir := self.WorkingNode + "/heartbeat"
	_, err := self.registry.registryClient.GetDirChildren(self.Group)
	if err != nil {
		log.Error("worker get [%s] group error", self.Group)
		self.Exit()
		return
	}
	//如果成员节点不存在，表示组已经被删除了
	agentHeartBeatValue, err := self.registry.registryClient.Get(hbdir)
	if err != nil {
		log.Error("worker get [%s] heartbeat error", hbdir)
		return
	}
	//检查当前工作节点的心跳
	isTimeOut, err := self.checkTimeout(self.WorkingNode, agentHeartBeatValue)

	//检查过后，刷新状态
	self.LastWorkingNode = self.WorkingNode

	//如果发生了超时现象:
	if err != nil || isTimeOut {
		//心跳异常,找新的替换这
		log.Info("%s heartbeat is not working, prepare to find node alived in %s ! ", self.LastWorkingNode, self.Group)

		timeoutEvt := &Exchange{
			WorkerGroup: self.Group,
			OpEvent:     StopEvent,
		}

		self.registry.exchangeChan <- timeoutEvt

		//找出替代工作的节点
		aliveNode, err := self.FindGroupAliveNode()
		if err != nil || aliveNode == "" {
			//没找到工作节点
			log.Error(err)
			return
		}
		//找到工作节点
		if aliveNode != "" {
			ex := &Exchange{
				From:        self.LastWorkingNode,
				To:          aliveNode,
				OpEvent:     UpdateEvent,
				WorkerGroup: self.Group,
			}

			//暂停工作
			self.StopWorking()

			//发送替代消息
			self.registry.exchangeChan <- ex
		}
	} else {
		//心跳正常
		log.Info("[%s] Worker Keepalive Success -> %s !", self.WorkingNode, self.LastKeepalive)
	}
}

//检查超时情况
func (self *AgentWorker) checkTimeout(agent, agentHb string) (bool, error) {
	hbs := strings.Split(agentHb, "-")
	if len(hbs) < 3 {
		return false, errors.New("worker get heartbeat value error")
	}
	//获取心跳的当前时间戳
	hb := hbs[2]
	if hb == "" {
		return false, errors.New("worker get heartbeat value null")
	}

	//取时间
	hbtm, err := strconv.ParseInt(hb, 10, 64)

	if err != nil {
		return false, errors.New("worker parse heartbeat value error")
	}

	//当前的心跳时间
	currentHeatBeatTime := time.Unix(hbtm, 0)
	if self.LastKeepalive.IsZero() {
		self.LastKeepalive = currentHeatBeatTime //记录心跳
		return false, nil
	}

	//self.LastWorkingNode = agent //上一个检查的节点
	subtime := currentHeatBeatTime.Sub(self.LastKeepalive)
	//时间间隔超过规定的区间
	if subtime < self.KeepalivePeriod {
		return true, nil
	} else {
		//没有超时
		self.LastKeepalive = currentHeatBeatTime
		return false, nil
	}
}

//找出组下存活的节点
func (self *AgentWorker) FindGroupAliveNode() (string, error) {
	members, err := self.registry.registryClient.GetDirChildren(self.Group + "/members")
	if err != nil {
		log.Error("[%s] No Members Found !", self.Group)
	}
	//找出能用的节点
	if len(members) > 0 {
		for _, member := range members {
			hbdir := member + "/heartbeat"
			agentHeartBeatValue, err := self.registry.registryClient.Get(hbdir)
			if err != nil {
				continue
			}
			isTimeOut, err := self.checkTimeout(member, agentHeartBeatValue)
			if err != nil || isTimeOut {
				continue
			}
			return member, nil
		}
	}
	errMsg := fmt.Sprintf("alive node not found in %s", self.Group)
	return "", errors.New(errMsg)
}

func (self *AgentWorker) Exit() {
	log.Info("worker exit now")
	self.WorkingNode = ""
	exitEvt := &Exchange{
		OpEvent:     ExitEvent,
		WorkerGroup: self.Group,
	}
	self.registry.exchangeChan <- exitEvt
}
