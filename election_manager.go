package electing_master

import (
	"errors"
	"github.com/samuel/go-zookeeper/zk"
	"log"
	"strings"
	"time"
)

type ZookeeperConfig struct {
	Servers    []string
	RootPath   string
	MasterPath string
}

type ElectionManager struct {
	ZKClientConn *zk.Conn
	ZKConfig     *ZookeeperConfig
	IsMasterQ    chan bool
}

const (
	DefaultRootPath   = "/GOLANG_ELECTING_MASTER"
	DefaultMasterPath = "/MASTER"
)

// zkAddr format: {ip1}:{port1},{ip2}:{port2},{ip3}:{port3}/{rootPath}, and the rootPath is the optional field
// zkAddr eg1: 127.0.0.1
// zkAddr eg2: 127.0.0.1:2080,127.0.0.1:2081,127.0.0.1:2082
// zkAddr eg3: 127.0.0.1:2080,127.0.0.1:2081,127.0.0.1:2082/__ELECTING_MASTER__
func GoElectingMaster(zkAddr string, isMasterQ chan bool) error {
	zkConf := &ZookeeperConfig{
		RootPath:   DefaultRootPath,
		MasterPath: DefaultMasterPath,
	}
	if zkAddr == "" {
		return errors.New("empty zookeeper address.")
	}
	var serversStr, pathStr string
	if strings.Contains(zkAddr, "/") {
		sp := strings.Split(zkAddr, "/")
		serversStr, pathStr = sp[0], sp[1]
	} else {
		serversStr = zkAddr
	}
	if pathStr != "" {
		zkConf.RootPath = "/" + pathStr
	}
	if strings.Contains(serversStr, ",") {
		zkConf.Servers = strings.Split(serversStr, ",")
	} else {
		zkConf.Servers = []string{serversStr}
	}
	electionManager := &ElectionManager{
		nil,
		zkConf,
		isMasterQ,
	}
	if err := electionManager.electMaster(); err != nil {
		return err
	}
	go electionManager.WatchMaster()
	return nil
}

// 判断是否成功连接到zookeeper
func (electionManager *ElectionManager) isConnected() bool {
	if electionManager.ZKClientConn == nil {
		return false
	} else if electionManager.ZKClientConn.State() != zk.StateConnected {
		return false
	}
	return true
}

// 初始化zookeeper连接
func (electionManager *ElectionManager) initConnection() error {
	// 连接为空，或连接不成功，获取zookeeper服务器的连接
	if !electionManager.isConnected() {
		conn, connChan, err := zk.Connect(electionManager.ZKConfig.Servers, time.Second)
		if err != nil {
			return err
		}
		// 等待连接成功
		for {
			isConnected := false
			select {
			case connEvent := <-connChan:
				if connEvent.State == zk.StateConnected {
					isConnected = true
					log.Println("connect to zookeeper server success!")
				}
			case _ = <-time.After(time.Second * 3): // 3秒仍未连接成功则返回连接超时
				return errors.New("connect to zookeeper server timeout!")
			}
			if isConnected {
				break
			}
		}
		electionManager.ZKClientConn = conn
	}
	return nil
}

// 选举master
func (electionManager *ElectionManager) electMaster() error {
	err := electionManager.initConnection()
	if err != nil {
		return err
	}
	// 判断zookeeper中是否存在root目录，不存在则创建该目录
	isExist, _, err := electionManager.ZKClientConn.Exists(electionManager.ZKConfig.RootPath)
	if err != nil {
		return err
	}
	if !isExist {
		path, err := electionManager.ZKClientConn.Create(electionManager.ZKConfig.RootPath, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return err
		}
		if electionManager.ZKConfig.RootPath != path {
			return errors.New("Create returned different path " + electionManager.ZKConfig.RootPath + " != " + path)
		}
	}

	// 创建用于选举master的ZNode，该节点为Ephemeral类型，表示客户端连接断开后，其创建的节点也会被销毁
	masterPath := electionManager.ZKConfig.RootPath + electionManager.ZKConfig.MasterPath
	path, err := electionManager.ZKClientConn.Create(masterPath, nil, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err == nil { // 创建成功表示选举master成功
		if path == masterPath {
			log.Println("elect master success!")
			electionManager.IsMasterQ <- true
		} else {
			return errors.New("Create returned different path " + masterPath + " != " + path)
		}
	} else { // 创建失败表示选举master失败
		log.Printf("elect master failure: %s", err)
		electionManager.IsMasterQ <- false
	}
	return nil
}

// 监听zookeeper中master znode，若被删除，表示master故障或网络迟缓，重新选举
func (electionManager *ElectionManager) WatchMaster() {
	// watch zk根znode下面的子znode，当有连接断开时，对应znode被删除，触发事件后重新选举
	children, state, childCh, err := electionManager.ZKClientConn.ChildrenW(electionManager.ZKConfig.RootPath + electionManager.ZKConfig.MasterPath)
	if err != nil {
		log.Printf("watch children error: %s", err)
	}
	log.Printf("watch children result. children=%s, state=%v", children, state)
	for {
		select {
		case childEvent := <-childCh:
			if childEvent.Type == zk.EventNodeDeleted {
				log.Printf("receive znode delete event: %v", childEvent)
				// 重新选举
				log.Println("start electing new master ...")
				err = electionManager.electMaster()
				if err != nil {
					log.Printf("elect new master error: %s", err)
				}
			}
		}
	}
}
