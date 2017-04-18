package etcd

import (
	"errors"
	"fmt"
	log "github.com/alecthomas/log4go"
	"strconv"
	"strings"
	"time"
)

const INCR int32 = 1

//服务服务Leader的Woker
//一个组一个worker
type LeaderWorker struct {
	registry        *EtcdRegistry
	Group           string
	WorkingNode     string
	LastWorkingNode string
	KeepalivePeriod time.Duration
	LastKeepalive   time.Time
}

//创建判官
func NewLeaderWorker(reg *EtcdRegistry, period time.Duration, group string) *LeaderWorker {
	return &LeaderWorker{
		registry:        reg,
		KeepalivePeriod: period,
		Group:           group}
}

func (self *LeaderWorker) StopWorking() {
	self.WorkingNode = ""
}

func (self *LeaderWorker) StartWorking() {
	leader := self.registry.GetGroupLeader(self.Group)
	if leader != "" {
		self.WorkingNode = leader
	}
}

//需要做得工作：
//1. 检查所属的组是否存在,如果不存在,则告诉注册器,把自己干掉
//2. 检查监控的agent是否还存在
//3. 检查监控的agent心跳
func (self *LeaderWorker) Keepalive() {
	if self.WorkingNode == "" {
		log.Info("[%s] Worker is not working", self.Group)
		return
	}

	hbdir := self.Group + "/members/" + self.WorkingNode + "/heartbeat"
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

	if agentHeartBeatValue == "" {
		//TODO: 找新的
	}

	//检查当前工作节点的心跳
	isTimeOut, err := self.checkTimeout(agentHeartBeatValue)

	//检查过后，刷新状态
	self.LastWorkingNode = self.WorkingNode

	//如果发生了超时现象:
	if err != nil || isTimeOut {
		//找出替代工作的节点
		aliveNode, err := self.FindGroupAliveNode()
		if err != nil || aliveNode == "" {
			//没找到工作节点
			log.Error(err)
			return
		}
		//找到工作节点
		if aliveNode != "" && aliveNode != self.GetNodeId(self.LastWorkingNode) {
			log.Info("[EXCHANGE] new leader was found [%s], request to update", aliveNode)
			ex := &Exchange{
				From:        self.LastWorkingNode,
				To:          self.GetNodeId(aliveNode),
				OpEvent:     UpdateEvent,
				WorkerGroup: self.Group,
			}
			//请求调度器进行替换
			self.registry.exchangeChan <- ex
		}
	} else {
		//心跳正常
		log.Info("[SUCCESS][%s/members/%s] ALIVED!", self.Group, self.WorkingNode)
	}
}

//检查超时情况
func (self *LeaderWorker) checkTimeout(agentHb string) (bool, error) {
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

func (self *LeaderWorker) GetNodeId(nodePath string) string {
	index := strings.Index(nodePath, "/members/")
	memberName := nodePath[index+len("/members/"):]
	return memberName
}

//找出组下存活的节点
func (self *LeaderWorker) FindGroupAliveNode() (string, error) {
	members, err := self.registry.registryClient.GetDirChildren(self.Group + "/members")
	if err != nil {
		log.Error("[%s] No Members Found !", self.Group)
	}
	//找出能用的节点
	if len(members) > 0 {
		for _, member := range members {

			index := strings.Index(member, "/members/")
			memberName := member[index+len("/members/"):]

			if memberName == self.WorkingNode {
				continue
			}
			hbdir := member + "/heartbeat"
			agentHeartBeatValue, err := self.registry.registryClient.Get(hbdir)
			if err != nil {
				continue
			}

			isTimeOut, err := self.checkTimeout(agentHeartBeatValue)

			if err != nil || isTimeOut {
				continue
			}
			return member, nil
		}
	}
	errMsg := fmt.Sprintf("[ERROR] No Alive Node For Leader Found In %s", self.Group)
	return "", errors.New(errMsg)
}

func (self *LeaderWorker) Exit() {
	log.Info("worker exit now")
	self.WorkingNode = ""
	exitEvt := &Exchange{
		OpEvent:     ExitEvent,
		WorkerGroup: self.Group,
	}
	self.registry.exchangeChan <- exitEvt
}
