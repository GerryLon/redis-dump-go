package redisdump

import (
	"context"
	"sync"

	"github.com/go-redis/redis/v8"
)

var (
	client   *redis.Client
	initOnce sync.Once
)

func newRedisClient(addr, password string) *redis.Client {
	initOnce.Do(func() {
		client = redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: password,
		})

		if err := client.Ping(context.Background()).Err(); err != nil {
			panic(err)
		}
	})

	return client
}

func CloseRedisClient() error {
	return client.Close()
}
