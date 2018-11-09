# golang实现通过zookeeper选举master

应用程序中有一个定时任务模块，之前是单机部署的，在实现多实例部署之后就对应用程序有了如下要求：
 - 一个定时任务只能在一个实例上运行
 - 一个应用实例故障后，原本由该实例执行的定时任务需要在其它实例上继续运行

Zookeeper 能够很容易实现这样的集群管理功能，它能够维护当前的集群中机器的服务状态，而且能够选出一个“总管”，让这个总管来管理集群，在这里即是让这个“总管”去执行定时任务。

##  1. go zk client准备

从网上找到 golang 的 zookeeper 客户端 go-zookeeper：https://github.com/samuel/go-zookeeper ，网上反响很不错，而且还有比较详细的文档 zk-GoDoc: https://godoc.org/github.com/samuel/go-zookeeper/zk 。

##  2. 选举

实现方式是在 zookeeper 上创建一个 EPHEMERAL 类型的目录节点，当有一个实例创建成功后，该目录节点便已经存在，其它实例再去创建该节点就会提示该节点已存在。这也是我们想要的结果，将在 zookeeper 上创建目录节点成功的实例选举为 master 。

## 3. 监听

然后每个 Server 在它们创建目录节点的父目录节点上获取并监听(getChildrenW)该节点，由于是 EPHEMERAL 目录节点，当创建它的 Server 死去，这个目录节点也随之被删除，所以 Children 将会变化，这时 getChildren上的 Watch 将会被调用，所以其它 Server 就知道已经有某台 Server 死去了。此时，执行上一步的选举步骤，即每个实例在 zookeeper 上创建目录节点，创建成功的实例被选举为 master 。

## 4. 测试

从github上拉取代码，将代码目录添加到GOPATH环境变量中，执行

> go get github.com/samuel/go-zookeeper/zk

获取 golang的zookeeper客户端，进入到代码目录后执行

> go run examples/main.go

启动程序，第一个启动显示如下日志

> connect to zookeeper server success! <br>
2017/04/13 15:03:12 Connected to 127.0.0.1:2181 <br>
2017/04/13 15:03:12 Authenticated: id=97784096665632770, timeout=4000 <br>
2017/04/13 15:03:12 Re-submitting \`0\` credentials after reconnect <br>
elect master success! <br>
do some job on master <br>
watch children result,  [] &{148 148 1492066992051 1492066992051 0 0 0 97784096665632770 0 0 148}<br>

显示选举成功。

根据选举master的步骤可想而知，一般是第一个启动的实例选举master成功。

保持该实例继续运行，在另一个命令行再次启动程序，显示日志如下：

> connect to zookeeper server success! <br>
2017/04/13 15:24:20 Connected to 127.0.0.1:2181 <br>
2017/04/13 15:24:20 Authenticated: id=97784096665632774, timeout=4000 <br>
2017/04/13 15:24:20 Re-submitting \`0\` credentials after reconnect <br>
elect master failure, %!(EXTRA \*errors.errorString=zk: node already exists)watch children result,  [] &{157 157 1492068257748 1492068257748 0 0 0 9778
4096665632773 0 0 157}

表示创建的节点已存在，选举master失败，这也是想要的结果。

现在模拟master节点故障的情况，停掉第一个启动的程序实例，第二个程序实例出现如下日志：

> receive znode delete event,  {EventNodeDeleted Unknown /GOLANG_ELECTING_MASTER/MASTER <nil> } <br>
start elect new master ... <br>
2017/04/13 15:27:50 Connected to 127.0.0.1:2181 <br>
connect to zookeeper server success! <br>
2017/04/13 15:27:50 Authenticated: id=97784096665632775, timeout=4000 <br>
2017/04/13 15:27:50 Re-submitting \`0\` credentials after reconnect <br>
elect master success! <br>
do some job on master <br>

表示程序重新选举了，此时只有一个节点，固然能够创建节点成功并选举成master，成为“管家”继续工作。

## 5. 使用

 ```golang
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
 ```
