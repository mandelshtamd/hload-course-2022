package dqueue

import (
	"context"
	"encoding/json"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
)

const (
	alphabet  = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
	separator = ":"
)

var suffs []string

type RedisValue struct {
	Value     string
	Timestamp int64
}

func init() {
	for _, a := range alphabet {
		for _, b := range alphabet {
			for _, c := range alphabet {
				suffs = append(suffs, string([]rune{a, b, c}))
			}
		}
	}
}

type redisClient struct {
	options *redis.ClusterOptions
}

func CreateRedisClient() *redisClient {
	return &redisClient{}
}

func (c *redisClient) ConfigureClient(redisOptions *redis.ClusterOptions) {
	c.options = redisOptions
}

func (c *redisClient) Create(name string, n int) ([]*Host, error) {
	client := redis.NewClusterClient(c.options)
	var result []*Host

	if n > 4 {
		return nil, TooMuchShardsErr
	}
	isUsed := make(map[string]bool)
	for _, addr := range c.options.Addrs {
		isUsed[addr] = false
	}

	clustersSlots, err := client.ClusterSlots(context.Background()).Result()
	if err != nil {
		return nil, InternalErr
	}

	for _, suff := range suffs {
		slot, err := client.ClusterKeySlot(context.Background(), name+separator+suff).Result()
		if err != nil {
			continue
		}

		for _, clusterSlot := range clustersSlots {
			if int64(clusterSlot.Start) <= slot && slot <= int64(clusterSlot.End) {
				if use, ok := isUsed[clusterSlot.Nodes[0].Addr]; ok && !use {
					isUsed[clusterSlot.Nodes[0].Addr] = true
					result = append(result, &Host{
						Addr: clusterSlot.Nodes[0].Addr,
						Key:  name + separator + suff,
					})
				}
			}
		}

		if len(result) == n {
			break
		}
	}

	if len(result) < n {
		return nil, InternalErr
	}

	return result, nil
}

func (c *redisClient) Push(host Host, value RedisValue) error {
	redisOptions := &redis.Options{
		Addr:     host.Addr,
		Password: "",
		DB:       0,
	}
	client := redis.NewClient(redisOptions)

	data, err := json.Marshal(value)
	if err != nil {
		return errors.Wrap(err, InternalErrMsg)
	}
	_, err = client.RPush(context.Background(), host.Key, string(data)).Result()
	return err
}

func (c *redisClient) Front(host *Host) (*RedisValue, error) {
	redisOptions := &redis.Options{
		Addr:     host.Addr,
		Password: "",
		DB:       0,
	}
	client := redis.NewClient(redisOptions)
	value, err := client.LIndex(context.Background(), host.Key, 0).Result()

	if err != nil {
		return nil, err
	}

	var result RedisValue
	err = json.Unmarshal([]byte(value), &result)
	if err != nil {
		return nil, err
	}

	return &result, nil
}

func (c *redisClient) Pop(host Host) error {
	redisOptions := &redis.Options{
		Addr:     host.Addr,
		Password: "",
		DB:       0,
	}
	client := redis.NewClient(redisOptions)
	_, err := client.LPop(context.Background(), host.Key).Result()
	return err
}
