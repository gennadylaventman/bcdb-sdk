package test

import "sync"

var portMutex sync.Mutex
var nodePortBase uint32 = 6001
var peerPortBase uint32 = 6101

func GetPorts() (nodePort, peerPort uint32) {
	portMutex.Lock()
	defer portMutex.Unlock()
	
	nodePort = nodePortBase
	peerPort = peerPortBase
	nodePortBase++
	peerPortBase++

	return
}
