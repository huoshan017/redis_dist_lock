package redis_dist_lock

import (
	"log"
	"time"

	"github.com/go-redis/redis"
)

type RedisDistLock struct {
	client     *redis.Client
	key        string
	expiration time.Duration
}

func NewRedisDistLock(addr string, key string, expiration time.Duration) *RedisDistLock {
	client := redis.NewClient(&redis.Options{
		Network: "tcp",
		Addr:    addr,
	})
	if client == nil {
		return nil
	}
	return &RedisDistLock{
		client:     client,
		key:        key,
		expiration: expiration,
	}
}

func (this *RedisDistLock) TryLock(rand_str string) (bool, error) {
	c := this.client.SetNX(this.key, rand_str, this.expiration)
	if c.Err() != nil {
		log.Printf("TryLock SetNX key %v with rand string %v expiration %v err: %v\n", this.key, rand_str, this.expiration, c.Err().Error())
		return false, c.Err()
	}
	if !c.Val() {
		return false, nil
	}
	return true, nil
}

func (this *RedisDistLock) Lock(rand_str string) error {
	for {
		b, err := this.TryLock(rand_str)
		if err != nil {
			return err
		}
		if b {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}
	return nil
}

func (this *RedisDistLock) Unlock(rand_str string) bool {
	c := this.client.Eval(delete_script, []string{this.key}, rand_str)
	if c.Err() != nil {
		log.Printf("Unlock %v for key %v err: %v\n", rand_str, this.key, c.Err().Error())
		return false
	}
	if c.Val().(int64) == 0 {
		return false
	}
	return true
}
