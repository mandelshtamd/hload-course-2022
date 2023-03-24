package dqueue

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/pkg/errors"
)

const (
	lockSuffix = "-lock-"
)

type zkConnection struct {
	zkHosts []string
	conn    *zk.Conn
}

func CreateZooKeeperClient() *zkConnection {
	return &zkConnection{}
}

func (z *zkConnection) ConfigClient(zkHosts []string) {
	z.zkHosts = zkHosts
}

func (z *zkConnection) Connect() (*zk.Conn, error) {
	connection, _, err := zk.Connect(z.zkHosts, time.Second)
	if err != nil {
		return nil, errors.Wrap(err, "Can not connect ZooKeeper")
	}

	return connection, nil
}

func (z *zkConnection) Lock(c *zk.Conn, path, queue string) (string, error) {
	name, err := c.Create(
		path+queue+lockSuffix, nil, zk.FlagEphemeral|zk.FlagSequence, zk.WorldACL(zk.PermAll))
	if err != nil {
		return "", errors.Wrap(err, "Can not create node")
	}

	for {
		children, _, err := c.Children(path[:len(path)-1])
		if err != nil {
			return "", errors.Wrap(err, InternalErrMsg)
		}

		lowestChild := name
		for _, child := range children {
			if strings.HasPrefix(child, path+queue+lockSuffix) {
				lowest, err := strconv.Atoi(strings.TrimPrefix(child, path+queue+lockSuffix))
				if err != nil {
					return "", errors.Wrap(err, InternalErrMsg)
				}
				suffix, err := strconv.Atoi(strings.TrimPrefix(child, path+queue+lockSuffix))
				if err != nil {
					return "", errors.Wrap(err, InternalErrMsg)
				}

				if suffix < lowest {
					lowestChild = child
					break
				}
			}
		}
		if lowestChild == name {
			return name, nil
		}

		exists, _, events, err := c.ExistsW(lowestChild)
		if err != nil {
			return "", errors.Wrap(err, InternalErrMsg)
		}

		if exists {
			_ = <-events
		}
	}
}

func (z *zkConnection) UnLock(c *zk.Conn, lock string) error {
	_, stat, err := c.Get(lock)
	if err != nil {
		return errors.Wrap(err, InternalErrMsg)
	}

	err = c.Delete(lock, stat.Version)
	if err != nil {
		return errors.Wrap(err, InternalErrMsg)
	}

	return nil
}

func (z *zkConnection) Get(c *zk.Conn, node string) (*QueueInfo, error) {
	data, _, err := c.Get(node)
	if err != nil {
		return nil, errors.Wrap(err, InternalErrMsg)
	}

	var result QueueInfo
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, InternalErr
	}

	return &result, nil
}

func (z *zkConnection) Create(c *zk.Conn, node string, q QueueInfo) error {
	data, err := json.Marshal(q)
	if err != nil {
		return errors.Wrap(err, InternalErrMsg)
	}

	_, err = c.Create(node, data, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		return errors.Wrap(err, "Can not create node")
	}

	return nil
}

func (z *zkConnection) Update(c *zk.Conn, node string, q QueueInfo) error {
	_, stat, err := c.Get(node)
	if err != nil {
		return errors.Wrap(err, InternalErrMsg)
	}

	data, err := json.Marshal(q)
	if err != nil {
		return errors.Wrap(err, InternalErrMsg)
	}

	_, err = c.Set(node, data, stat.Version)
	if err != nil {
		return errors.Wrap(err, InternalErrMsg)
	}

	return nil
}
