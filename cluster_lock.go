package redis_dist_lock

import (
	"log"
	"time"

	"github.com/go-redis/redis"
)

type RedisClusterDistLock struct {
	cluster    *redis.ClusterClient
	key        string
	expiration time.Duration
}

func NewRedisClusterDistLock(addrs []string, key string, expiration time.Duration) *RedisClusterDistLock {
	cluster := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: addrs,
	})
	if cluster == nil {
		return nil
	}
	return &RedisClusterDistLock{
		cluster:    cluster,
		key:        key,
		expiration: expiration,
	}
}

var check_pttl_delete_script = `
	if redis.call("GET", KEYS[1]) ~= "" then
		if redis.call("PTTL", KEYS[1]) <= 0 then
			redis.call("EXPIRE", KEYS[1], ARGV[2])
			return redis.call("SET", KEYS[1], ARGV[1])
		end
	end
	return ""
`

func (this *RedisClusterDistLock) TryLock(rand_str string) (bool, error) {
	c := this.cluster.SetNX(this.key, rand_str, this.expiration)
	if c.Err() != nil {
		log.Printf("TryLock SetNX key %v with rand string %v expiration %v err: %v\n", this.key, rand_str, this.expiration, c.Err().Error())
		return false, c.Err()
	}
	if !c.Val() {
		bc := this.cluster.Eval(check_pttl_delete_script, []string{this.key}, rand_str, this.expiration)
		if bc.Err() != nil {
			return false, c.Err()
		}
		if bc.Val().(string) == "" {
			return false, nil
		}
	}
	return true, nil
}

func (this *RedisClusterDistLock) Lock(rand_str string) error {
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

var delete_script = `
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("DEL", KEYS[1])
	else
		return 0
	end
`

func (this *RedisClusterDistLock) Unlock(rand_str string) bool {
	c := this.cluster.Eval(delete_script, []string{this.key}, rand_str)
	if c.Err() != nil {
		log.Printf("Unlock %v for key %v err: %v\n", rand_str, this.key, c.Err().Error())
		return false
	}
	if c.Val().(int64) == 0 {
		return false
	}
	return true
}
