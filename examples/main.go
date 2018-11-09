package main

import (
	em "brotherbin/electing-master"
	"log"
)

func main() {
	zkAddr := "10.202.7.191:2181"
	isMasterChan := make(chan bool, 10) // main goroutine 和 electing goroutine之间通信的channel，用于返回选举结果
	var isMaster bool
	err := em.GoElectingMaster(zkAddr, isMasterChan)
	if err != nil {
		log.Panicf("new election manager error: %s", err)
	}
	for {
		log.Printf("is master: %v", isMaster)
		select {
		case isMaster = <-isMasterChan:
			if isMaster {
				log.Println("do some job on master")
			} else {
				log.Println("standby for some job")
			}
		}
	}
}
