package redisdump

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

var (
	bgCtx = context.Background()
)

// 将字符串中的单引号替换为 \' 将整个字符串用单引号引起来
func quote(s ...string) []string {
	for i := range s {
		// if strings.Contains(s[i], "'") {
			s[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(s[i], `'`, `\'`))
		// }
	}
	return s
}

func ttlToRedisCmd(k string, val int64) []string {
	return []string{"EXPIREAT", k, fmt.Sprint(time.Now().Unix() + val)}
}

func stringToRedisCmd(k, val string) []string {
	return []string{"SET", k, quote(val)[0]}
}

func hashToRedisCmd(k string, val map[string]string) []string {
	cmd := []string{"HSET", k}
	for k, v := range val {
		cmd = append(cmd, k, quote(v)[0])
	}
	return cmd
}

func setToRedisCmd(k string, val []string) []string {
	cmd := []string{"SADD", k}

	return append(cmd, quote(val...)...)
}

func listToRedisCmd(k string, val []string) []string {
	cmd := []string{"RPUSH", k}
	return append(cmd, quote(val...)...)
}

func zsetToRedisCmd(k string, val []string) []string {
	cmd := []string{"ZADD", k}
	var key string

	for i, v := range val {
		if i%2 == 0 {
			key = v
			continue
		}

		cmd = append(cmd, v, quote(key)[0])
	}
	return cmd
}

// RESPSerializer will serialize cmd to RESP
func RESPSerializer(cmd []string) string {
	s := ""
	s += "*" + strconv.Itoa(len(cmd)) + "\r\n"
	for _, arg := range cmd {
		s += "$" + strconv.Itoa(len(arg)) + "\r\n"
		s += arg + "\r\n"
	}
	return s
}

// RedisCmdSerializer will serialize cmd to a string with redis commands
func RedisCmdSerializer(cmd []string) string {
	return strings.Join(cmd, " ")
}

func dumpKeys(client *redis.Client, keys []string, withTTL bool, logger *log.Logger,
	serializer func([]string) string) error {
	var err error
	var redisCmd []string

	for _, key := range keys {
		var keyType string

		statusCmd := client.Type(bgCtx, key)
		if err = statusCmd.Err(); err != nil {
			return err
		}
		keyType = statusCmd.Val()

		var stringCmd *redis.StringCmd
		var stringSliceCmd *redis.StringSliceCmd
		var stringStringMapCmd *redis.StringStringMapCmd
		var durationCmd *redis.DurationCmd

		switch keyType {
		case "string":
			var val string
			stringCmd = client.Get(bgCtx, key)
			if err = stringCmd.Err(); err != nil {
				return err
			}
			val = stringCmd.Val()
			redisCmd = stringToRedisCmd(key, val)

		case "list":
			var val []string
			stringSliceCmd = client.LRange(bgCtx, key, 0, -1)
			if err = stringSliceCmd.Err(); err != nil {
				return err
			}
			val = stringSliceCmd.Val()
			redisCmd = listToRedisCmd(key, val)

		case "set":
			var val []string
			stringSliceCmd = client.SMembers(bgCtx, key)
			if err = stringSliceCmd.Err(); err != nil {
				return err
			}
			val = stringSliceCmd.Val()
			redisCmd = setToRedisCmd(key, val)

		case "hash":
			var val map[string]string
			stringStringMapCmd = client.HGetAll(bgCtx, key)
			if err = stringStringMapCmd.Err(); err != nil {
				return err
			}
			val = stringStringMapCmd.Val()
			redisCmd = hashToRedisCmd(key, val)

		case "zset":
			var val []string
			opt := &redis.ZRangeBy{
				Min: "-inf",
				Max: "+inf",
			}
			stringSliceCmd = client.ZRangeByScore(bgCtx, key, opt)
			if err = stringSliceCmd.Err(); err != nil {
				return err
			}
			val = stringSliceCmd.Val()
			redisCmd = zsetToRedisCmd(key, val)

		case "none":

		default:
			return fmt.Errorf("key %s is of unreconized type %s", key, keyType)
		}

		logger.Print(serializer(redisCmd))

		if withTTL {
			var ttl int64
			durationCmd = client.TTL(bgCtx, key)
			if err = durationCmd.Err(); err != nil {
				return err
			}
			ttl = int64(durationCmd.Val())
			if ttl > 0 {
				redisCmd = ttlToRedisCmd(key, ttl)
				logger.Print(serializer(redisCmd))
			}
		}
	}

	return nil
}

func dumpKeysWorker(client *redis.Client, keyBatches <-chan []string, withTTL bool, logger *log.Logger,
	serializer func([]string) string, errors chan<- error, done chan<- bool) {
	for keyBatch := range keyBatches {
		if err := dumpKeys(client, keyBatch, withTTL, logger, serializer); err != nil {
			errors <- err
		}
	}
	done <- true
}

// ProgressNotification message indicates the progress in dumping the Redis server,
// and can be used to provide a progress visualisation such as a progress bar.
// Done is the number of items dumped, Total is the total number of items to dump.
type ProgressNotification struct {
	Db   uint8
	Done int
}

func parseKeyspaceInfo(keyspaceInfo string) ([]uint8, error) {
	var dbs []uint8

	scanner := bufio.NewScanner(strings.NewReader(keyspaceInfo))

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if !strings.HasPrefix(line, "db") {
			continue
		}

		dbIndexString := line[2:strings.IndexAny(line, ":")]
		dbIndex, err := strconv.ParseUint(dbIndexString, 10, 8)
		if err != nil {
			return nil, err
		}
		if dbIndex > 16 {
			return nil, fmt.Errorf("Error parsing INFO keyspace")
		}

		dbs = append(dbs, uint8(dbIndex))
	}

	return dbs, nil
}

// addr: host:port
func getDBIndexes(addr, password string) ([]uint8, error) {
	client := newRedisClient(addr, password)

	var keyspaceInfo string
	cmd := client.Info(bgCtx, "keyspace")
	if err := cmd.Err(); err != nil {
		return nil, err
	}
	keyspaceInfo = cmd.Val()

	return parseKeyspaceInfo(keyspaceInfo)
}

func scanKeys(client *redis.Client, db uint8, filter string, keyBatches chan<- []string,
	progressNotifications chan<- ProgressNotification) error {
	var keyBatchSize int = 100
	scanCmd := client.Scan(bgCtx, 0, filter, int64(keyBatchSize))
	// s := radix.NewScanner(client, radix.ScanOpts{Command: "SCAN", Pattern: filter, Count: keyBatchSize})
	if err := scanCmd.Err(); err != nil {
		log.Println(err)
		return err
	}

	var key string
	var keyBatch []string
	nProcessed := 0
	iter := client.Scan(bgCtx, 0, filter, int64(keyBatchSize)).Iterator()
	for iter.Next(bgCtx) {
		key = iter.Val()
		keyBatch = append(keyBatch, key)
		if len(keyBatch) >= keyBatchSize {
			nProcessed += len(keyBatch)
			keyBatches <- keyBatch
			keyBatch = nil
			progressNotifications <- ProgressNotification{Db: db, Done: nProcessed}
		}
	}
	if err := iter.Err(); err != nil {
		log.Println(err)
		return err
	}

	keyBatches <- keyBatch
	nProcessed += len(keyBatch)
	progressNotifications <- ProgressNotification{Db: db, Done: nProcessed}

	return nil
}

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

func scanKeysLegacy(client *redis.Client, db uint8, filter string, keyBatches chan<- []string,
	progressNotifications chan<- ProgressNotification) error {
	keyBatchSize := 100
	var err error
	cmd := client.Keys(bgCtx, filter)
	var keys []string
	if err = cmd.Err(); err != nil {
		return err
	}
	keys = cmd.Val()

	for i := 0; i < len(keys); i += keyBatchSize {
		batchEnd := min(i+keyBatchSize, len(keys))
		keyBatches <- keys[i:batchEnd]
		if progressNotifications != nil {
			progressNotifications <- ProgressNotification{db, i}
		}
	}

	return nil
}

// // RedisURL builds a connect URL given a Host, port, db & password
// func RedisURL(redisHost string, redisPort string, redisDB string, redisPassword string) string {
// 	switch {
// 	case redisDB == "":
// 		return "redis://:" + redisPassword + "@" + redisHost + ":" + fmt.Sprint(redisPort)
// 	case redisDB != "":
// 		return "redis://:" + redisPassword + "@" + redisHost + ":" + fmt.Sprint(redisPort) + "/" + redisDB
// 	}
//
// 	return ""
// }

// DumpDB dumps all keys from a single Redis DB
func DumpDB(redisHost string, redisPort int, redisPassword string, db uint8, filter string, nWorkers int, withTTL bool, noscan bool, logger *log.Logger, serializer func([]string) string, progress chan<- ProgressNotification) error {
	var err error

	keyGenerator := scanKeys
	if noscan {
		keyGenerator = scanKeysLegacy
	}

	errors := make(chan error)
	nErrors := 0
	go func() {
		for err := range errors {
			fmt.Fprintln(os.Stderr, "Error: "+err.Error())
			nErrors++
		}
	}()

	client := newRedisClient(fmt.Sprintf("%s:%d", redisHost, redisPort), redisPassword)
	cmd := client.Do(bgCtx, "SELECT", fmt.Sprint(db))
	if err = cmd.Err(); err != nil {
		return err
	}
	logger.Printf(serializer([]string{"SELECT", fmt.Sprint(db)}))

	done := make(chan bool)
	keyBatches := make(chan []string)
	for i := 0; i < nWorkers; i++ {
		go dumpKeysWorker(client, keyBatches, withTTL, logger, serializer, errors, done)
	}

	keyGenerator(client, db, filter, keyBatches, progress)
	close(keyBatches)

	for i := 0; i < nWorkers; i++ {
		<-done
	}

	return nil
}

// DumpServer dumps all Keys from the redis server given by redisURL,
// to the Logger logger. Progress notification informations
// are regularly sent to the channel progressNotifications
func DumpServer(redisHost string, redisPort int, redisPassword string, filter string, nWorkers int, withTTL bool, noscan bool, logger *log.Logger, serializer func([]string) string, progress chan<- ProgressNotification) error {
	// url := RedisURL(redisHost, fmt.Sprint(redisPort), "", redisPassword)
	dbs, err := getDBIndexes(fmt.Sprintf("%s:%d", redisHost, redisPort), redisPassword)
	if err != nil {
		return err
	}

	for _, db := range dbs {
		if err = DumpDB(redisHost, redisPort, redisPassword, db, filter, nWorkers, withTTL, noscan, logger, serializer, progress); err != nil {
			return err
		}
	}

	return nil
}
