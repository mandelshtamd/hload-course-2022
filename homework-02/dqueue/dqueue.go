package dqueue

import (
	"errors"
	redisLib "github.com/go-redis/redis/v8"
	"github.com/go-zookeeper/zk"
)

/*
 * Можно создавать несколько очередей
 *
 * Для клиента они различаются именами
 *
 * В реализации они могут потребовать вспомогательных данных
 * Для них - эта структура. Можете определить в ней любые поля
 */
type DQueue struct {
	name      string
	zkName    string
	redisName string
}

type QueueInfo struct {
	Shards    []*Host
	Current   int
	Timestamp int64
}

type Host struct {
	Addr string
	Key  string
}

var (
	zkPrefix    = "/dmandelshtam/queues/"
	redisPrefix = "dmandelshtam:"
	ZkClient    = CreateZooKeeperClient()
	RedisClient = CreateRedisClient()
)

/*
 * Запомнить данные и везде использовать
 */
func Config(redisOptions *redisLib.ClusterOptions, zkOptions []string) {
	ZkClient.ConfigClient(zkOptions)
	RedisClient.ConfigureClient(redisOptions)
}

/*
 * Открываем очередь на nShards шардах
 *
 * Попытка создать очередь с существующим именем и другим количеством шардов
 * должна приводить к ошибке
 *
 * При попытке создать очередь с существующим именем и тем же количеством шардов
 * нужно вернуть экземпляр DQueue, позволяющий делать Push/Pull
 *
 * Предыдущее открытие может быть совершено другим клиентом, соединенным с любым узлом
 * Redis-кластера
 *
 * Отдельные узлы Redis-кластера могут выпадать. Availability очереди в целом
 * не должна от этого страдать
 *
 */
func Open(name string, nShards int) (resQ DQueue, resErr error) {
	connection, err := ZkClient.Connect()
	if err != nil {
		return DQueue{}, err
	}

	redisPath := redisPrefix + name
	zkPath := zkPrefix + name
	lock, err := ZkClient.Lock(connection, zkPrefix, name)
	if err != nil {
		return DQueue{}, err
	}
	defer func() {
		err = ZkClient.UnLock(connection, lock)
		if err != nil {
			resErr = err
		}
	}()

	if queueInfo, err := ZkClient.Get(connection, zkPath); err != nil {
		if !errors.Is(err, zk.ErrNoNode) {
			return DQueue{}, err
		}
	} else {
		if len(queueInfo.Shards) != nShards {
			return DQueue{}, errors.New("Incorrect number of shards")
		}
		return DQueue{
			zkName:    zkPath,
			redisName: redisPath,
		}, nil
	}

	shards, err := RedisClient.Create(redisPath, nShards)
	if err != nil {
		return DQueue{}, err
	}

	info := QueueInfo{
		Shards:    shards,
		Current:   0,
		Timestamp: 1,
	}
	err = ZkClient.Create(connection, zkPath, info)
	if err != nil {
		return DQueue{}, err
	}

	return DQueue{
		name:      name,
		zkName:    zkPath,
		redisName: redisPath,
	}, nil
}

/*
 * Пишем в очередь. Каждый следующий Push - в следующий шард
 *
 * Если шард упал - пропускаем шард, пишем в следующий по очереди
 */
func (q *DQueue) Push(value string) (resErr error) {
	connection, err := ZkClient.Connect()
	if err != nil {
		return err
	}

	lock, err := ZkClient.Lock(connection, zkPrefix, q.name)
	if err != nil {
		return err
	}
	defer func() {
		err = ZkClient.UnLock(connection, lock)
		if err != nil {
			resErr = err
		}
	}()

	queueInfo, err := ZkClient.Get(connection, q.zkName)
	if err != nil {
		return err
	}

	tryNumber := 0
	for {
		tryNumber++

		err = RedisClient.Push(
			*queueInfo.Shards[queueInfo.Current],
			RedisValue{Value: value, Timestamp: queueInfo.Timestamp},
		)
		queueInfo.Current = (queueInfo.Current + 1) % len(queueInfo.Shards)
		if err == nil {
			break
		} else if tryNumber >= len(queueInfo.Shards) {
			return errors.New("No shards available")
		}
	}

	queueInfo.Timestamp += 1
	err = ZkClient.Update(connection, q.zkName, *queueInfo)
	if err != nil {
		return err
	}

	return nil
}

/*
 * Читаем из очереди
 *
 * Из того шарда, в котором самое раннее сообщение
 *
 */
func (q *DQueue) Pull() (resAns string, resErr error) {
	conn, err := ZkClient.Connect()
	if err != nil {
		return "", err
	}

	lock, err := ZkClient.Lock(conn, zkPrefix, q.name)
	if err != nil {
		return "", err
	}
	defer func() {
		err = ZkClient.UnLock(conn, lock)
		if err != nil {
			resErr = err
		}
	}()

	queueInfo, err := ZkClient.Get(conn, q.zkName)
	if err != nil {
		return "", err
	}

	for {
		var firstShard *Host
		firstValue := &RedisValue{
			Timestamp: queueInfo.Timestamp,
		}

		for _, shard := range queueInfo.Shards {
			value, err := RedisClient.Front(shard)
			if err == nil {
				if value.Timestamp < firstValue.Timestamp {
					firstValue = value
					firstShard = shard
				}
			}
		}

		if firstShard == nil {
			return "", QueueIsEmptyErr
		}

		err = RedisClient.Pop(*firstShard)
		if err != nil {
			continue
		}

		return firstValue.Value, nil
	}
}
