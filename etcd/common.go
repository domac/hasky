package etcd

import (
	"time"
)

type RegistryEvent byte

type OperationEvent byte

const (
	DISCOVERY = "/apus/agent-groups"

	CHECK_ALIVE_INTERVAL = 3 * time.Second
)

const (
	Created RegistryEvent = 1
	Deleted RegistryEvent = 2
	Changed RegistryEvent = 3
	Child   RegistryEvent = 4

	UpdateEvent OperationEvent = 1
	ExitEvent   OperationEvent = 2
	StopEvent   OperationEvent = 3
)
