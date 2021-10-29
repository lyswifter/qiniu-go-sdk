package operation

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/qiniupd/qiniu-go-sdk/api.v7/auth/qbox"
	"github.com/qiniupd/qiniu-go-sdk/api.v7/kodo"
)

var (
	ErrUndefinedConfig                        = errors.New("undefined config")
	ErrCannotTransferBetweenDifferentClusters = errors.New("cannot transfer between different clusters")
)

// 列举器
type Lister struct {
	config                   Configurable
	singleClusterLister      *singleClusterLister
	multiClustersConcurrency int
}

// 根据配置创建列举器
func NewLister(c *Config) *Lister {
	return &Lister{config: c, singleClusterLister: newSingleClusterLister(c)}
}

// 根据环境变量创建列举器
func NewListerV2() *Lister {
	c := getCurrentConfigurable()
	if c == nil {
		return nil
	} else if singleClusterConfig, ok := c.(*Config); ok {
		return NewLister(singleClusterConfig)
	} else {
		var (
			concurrency = 1
			err         error
		)
		if concurrencyStr := os.Getenv("QINIU_MULTI_CLUSTERS_CONCURRENCY"); concurrencyStr != "" {
			if concurrency, err = strconv.Atoi(concurrencyStr); err != nil {
				elog.Warn("Invalid QINIU_MULTI_CLUSTERS_CONCURRENCY: ", err)
			}
		}
		return &Lister{config: c, multiClustersConcurrency: concurrency}
	}
}

// 文件元信息
type FileStat struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

// 重命名对象
func (l *Lister) Rename(fromKey, toKey string) error {
	var scl *singleClusterLister
	if l.singleClusterLister != nil {
		scl = l.singleClusterLister
	} else {
		c, err := l.canTransfer(fromKey, toKey)
		if err != nil {
			return err
		}
		scl = newSingleClusterLister(c)
	}
	return scl.rename(fromKey, toKey)
}

// 移动对象到指定存储空间的指定对象中
func (l *Lister) MoveTo(fromKey, toBucket, toKey string) error {
	var scl *singleClusterLister
	if l.singleClusterLister != nil {
		scl = l.singleClusterLister
	} else {
		c, err := l.canTransfer(fromKey, toKey)
		if err != nil {
			return err
		}
		scl = newSingleClusterLister(c)
	}
	return scl.moveTo(fromKey, toBucket, toKey)
}

// 复制对象到当前存储空间的指定对象中
func (l *Lister) Copy(fromKey, toKey string) error {
	var scl *singleClusterLister
	if l.singleClusterLister != nil {
		scl = l.singleClusterLister
	} else {
		c, err := l.canTransfer(fromKey, toKey)
		if err != nil {
			return err
		}
		scl = newSingleClusterLister(c)
	}
	return scl.copy(fromKey, toKey)
}

func (l *Lister) canTransfer(fromKey, toKey string) (*Config, error) {
	configOfFromKey, exists := l.config.forKey(fromKey)
	if !exists {
		return nil, ErrUndefinedConfig
	}
	configOfToKey, exists := l.config.forKey(toKey)
	if !exists {
		return nil, ErrUndefinedConfig
	}
	if configOfFromKey != configOfToKey {
		return nil, ErrCannotTransferBetweenDifferentClusters
	}
	return configOfFromKey, nil
}

// 删除指定对象
func (l *Lister) Delete(key string) error {
	var scl *singleClusterLister
	if l.singleClusterLister != nil {
		scl = l.singleClusterLister
	} else {
		c, exists := l.config.forKey(key)
		if !exists {
			return ErrUndefinedConfig
		}
		scl = newSingleClusterLister(c)
	}
	return scl.delete(key)
}

// 获取指定对象列表的元信息
func (l *Lister) ListStat(keys []string) []*FileStat {
	if fileStats, err := l.listStat(context.Background(), keys); err != nil {
		return []*FileStat{}
	} else {
		return fileStats
	}
}

func (l *Lister) listStat(ctx context.Context, keys []string) ([]*FileStat, error) {
	if l.singleClusterLister != nil {
		return l.singleClusterLister.listStat(ctx, keys)
	}

	type KeysWithIndex struct {
		IndexMap []int
		Keys     []string
	}

	clusterPathsMap := make(map[*Config]*KeysWithIndex)
	for i, key := range keys {
		config, exists := l.config.forKey(key)
		if !exists {
			return nil, ErrUndefinedConfig
		}
		if keysWithIndex := clusterPathsMap[config]; keysWithIndex != nil {
			keysWithIndex.IndexMap = append(keysWithIndex.IndexMap, i)
			keysWithIndex.Keys = append(keysWithIndex.Keys, key)
		} else {
			keysWithIndex = &KeysWithIndex{Keys: make([]string, 0, 1), IndexMap: make([]int, 0, 1)}
			keysWithIndex.IndexMap = append(keysWithIndex.IndexMap, i)
			keysWithIndex.Keys = append(keysWithIndex.Keys, key)
			clusterPathsMap[config] = keysWithIndex
		}
	}

	pool := newGoroutinePool(l.multiClustersConcurrency)
	allStats := make([]*FileStat, len(keys))
	for config, keysWithIndex := range clusterPathsMap {
		func(config *Config, keys []string, indexMap []int) {
			pool.Go(func(ctx context.Context) error {
				if stats, err := l.listStatForConfig(ctx, config, keys); err != nil {
					return err
				} else {
					for i := range stats {
						allStats[indexMap[i]] = stats[i]
					}
					return nil
				}
			})
		}(config, keysWithIndex.Keys, keysWithIndex.IndexMap)
	}
	err := pool.Wait(context.Background())
	return allStats, err
}

func (l *Lister) listStatForConfig(ctx context.Context, config *Config, keys []string) ([]*FileStat, error) {
	return newSingleClusterLister(config).listStat(ctx, keys)
}

// 根据前缀列举存储空间
func (l *Lister) ListPrefix(prefix string) []string {
	if keys, err := l.listPrefix(context.Background(), prefix); err != nil {
		return []string{}
	} else {
		return keys
	}
}

func (l *Lister) listPrefix(ctx context.Context, prefix string) ([]string, error) {
	if l.singleClusterLister != nil {
		return l.singleClusterLister.listPrefix(ctx, prefix)
	}

	pool := newGoroutinePool(l.multiClustersConcurrency)
	allKeys := make([]string, 0)
	var allKeysMutex sync.Mutex
	l.config.forEachClusterConfig(func(config *Config) error {
		pool.Go(func(ctx context.Context) error {
			if keys, err := l.listPrefixForConfig(ctx, config, prefix); err != nil {
				return err
			} else {
				allKeysMutex.Lock()
				allKeys = append(allKeys, keys...)
				allKeysMutex.Unlock()
				return nil
			}
		})
		return nil
	})
	err := pool.Wait(ctx)
	return allKeys, err
}

func (l *Lister) listPrefixForConfig(ctx context.Context, config *Config, prefix string) ([]string, error) {
	return newSingleClusterLister(config).listPrefix(ctx, prefix)
}

func newSingleClusterLister(c *Config) *singleClusterLister {
	mac := qbox.NewMac(c.Ak, c.Sk)

	var queryer *Queryer = nil

	if len(c.UcHosts) > 0 {
		queryer = NewQueryer(c)
	}

	lister := singleClusterLister{
		bucket:           c.Bucket,
		rsHosts:          dupStrings(c.RsHosts),
		upHosts:          dupStrings(c.UpHosts),
		rsfHosts:         dupStrings(c.RsfHosts),
		credentials:      mac,
		queryer:          queryer,
		batchConcurrency: c.BatchConcurrency,
		batchSize:        c.BatchSize,
	}
	if lister.batchConcurrency <= 0 {
		lister.batchConcurrency = 20
	}
	if lister.batchSize <= 0 {
		lister.batchSize = 100
	}
	shuffleHosts(lister.rsHosts)
	shuffleHosts(lister.rsfHosts)
	shuffleHosts(lister.upHosts)
	return &lister
}

type singleClusterLister struct {
	bucket           string
	rsHosts          []string
	upHosts          []string
	rsfHosts         []string
	credentials      *qbox.Mac
	queryer          *Queryer
	batchSize        int
	batchConcurrency int
}

var curRsHostIndex uint32 = 0

func (l *singleClusterLister) nextRsHost(failedHosts map[string]struct{}) string {
	rsHosts := l.rsHosts
	if l.queryer != nil {
		if hosts := l.queryer.QueryRsHosts(false); len(hosts) > 0 {
			shuffleHosts(hosts)
			rsHosts = hosts
		}
	}
	switch len(rsHosts) {
	case 0:
		panic("No Rs hosts is configured")
	case 1:
		return rsHosts[0]
	default:
		var rsHost string
		for i := 0; i <= len(rsHosts)*MaxFindHostsPrecent/100; i++ {
			index := int(atomic.AddUint32(&curRsHostIndex, 1) - 1)
			rsHost = rsHosts[index%len(rsHosts)]
			if _, isFailedBefore := failedHosts[rsHost]; !isFailedBefore && isHostNameValid(rsHost) {
				break
			}
		}
		return rsHost
	}
}

var curRsfHostIndex uint32 = 0

func (l *singleClusterLister) nextRsfHost(failedHosts map[string]struct{}) string {
	rsfHosts := l.rsfHosts
	if l.queryer != nil {
		if hosts := l.queryer.QueryRsfHosts(false); len(hosts) > 0 {
			shuffleHosts(hosts)
			rsfHosts = hosts
		}
	}
	switch len(rsfHosts) {
	case 0:
		panic("No Rsf hosts is configured")
	case 1:
		return rsfHosts[0]
	default:
		var rsfHost string
		for i := 0; i <= len(rsfHosts)*MaxFindHostsPrecent/100; i++ {
			index := int(atomic.AddUint32(&curRsfHostIndex, 1) - 1)
			rsfHost = rsfHosts[index%len(rsfHosts)]
			if _, isFailedBefore := failedHosts[rsfHost]; !isFailedBefore && isHostNameValid(rsfHost) {
				break
			}
		}
		return rsfHost
	}
}

func (l *singleClusterLister) rename(fromKey, toKey string) error {
	failedRsHosts := make(map[string]struct{})
	host := l.nextRsHost(failedRsHosts)
	bucket := l.newBucket(host, "")
	err := bucket.Move(nil, fromKey, toKey)
	if err != nil {
		failedRsHosts[host] = struct{}{}
		failHostName(host)
		elog.Info("rename retry 0", host, err)
		host = l.nextRsHost(failedRsHosts)
		bucket = l.newBucket(host, "")
		err = bucket.Move(nil, fromKey, toKey)
		if err != nil {
			failedRsHosts[host] = struct{}{}
			failHostName(host)
			elog.Info("rename retry 1", host, err)
			return err
		} else {
			succeedHostName(host)
		}
	} else {
		succeedHostName(host)
	}
	return nil
}

func (l *singleClusterLister) moveTo(fromKey, toBucket, toKey string) error {
	failedRsHosts := make(map[string]struct{})
	host := l.nextRsHost(failedRsHosts)
	bucket := l.newBucket(host, "")
	err := bucket.MoveEx(nil, fromKey, toBucket, toKey)
	if err != nil {
		failedRsHosts[host] = struct{}{}
		failHostName(host)
		elog.Info("move retry 0", host, err)
		host = l.nextRsHost(failedRsHosts)
		bucket = l.newBucket(host, "")
		err = bucket.MoveEx(nil, fromKey, toBucket, toKey)
		if err != nil {
			failedRsHosts[host] = struct{}{}
			failHostName(host)
			elog.Info("move retry 1", host, err)
			return err
		} else {
			succeedHostName(host)
		}
	} else {
		succeedHostName(host)
	}
	return nil
}

func (l *singleClusterLister) copy(fromKey, toKey string) error {
	failedRsHosts := make(map[string]struct{})
	host := l.nextRsHost(failedRsHosts)
	bucket := l.newBucket(host, "")
	err := bucket.Copy(nil, fromKey, toKey)
	if err != nil {
		failedRsHosts[host] = struct{}{}
		failHostName(host)
		elog.Info("copy retry 0", host, err)
		host = l.nextRsHost(failedRsHosts)
		bucket = l.newBucket(host, "")
		err = bucket.Copy(nil, fromKey, toKey)
		if err != nil {
			failedRsHosts[host] = struct{}{}
			failHostName(host)
			elog.Info("copy retry 1", host, err)
			return err
		} else {
			succeedHostName(host)
		}
	} else {
		succeedHostName(host)
	}
	return nil
}

func (l *singleClusterLister) delete(key string) error {
	failedRsHosts := make(map[string]struct{})
	host := l.nextRsHost(failedRsHosts)
	bucket := l.newBucket(host, "")
	err := bucket.Delete(nil, key)
	if err != nil {
		failedRsHosts[host] = struct{}{}
		failHostName(host)
		elog.Info("delete retry 0", host, err)
		host = l.nextRsHost(failedRsHosts)
		bucket = l.newBucket(host, "")
		err = bucket.Delete(nil, key)
		if err != nil {
			failedRsHosts[host] = struct{}{}
			failHostName(host)
			elog.Info("delete retry 1", host, err)
			return err
		} else {
			succeedHostName(host)
		}
	} else {
		succeedHostName(host)
	}
	return nil
}

func (l *singleClusterLister) listStat(ctx context.Context, paths []string) ([]*FileStat, error) {
	concurrency := (len(paths) + l.batchSize - 1) / l.batchSize
	if concurrency > l.batchConcurrency {
		concurrency = l.batchConcurrency
	}
	var (
		stats             = make([]*FileStat, len(paths))
		failedRsHosts     = make(map[string]struct{})
		failedRsHostsLock sync.RWMutex
		pool              = newGoroutinePool(concurrency)
	)

	for i := 0; i < len(paths); i += l.batchSize {
		size := l.batchSize
		if size > len(paths)-i {
			size = len(paths) - i
		}
		func(paths []string, index int) {
			pool.Go(func(ctx context.Context) error {
				failedRsHostsLock.RLock()
				host := l.nextRsHost(failedRsHosts)
				failedRsHostsLock.RUnlock()
				bucket := l.newBucket(host, "")
				r, err := bucket.BatchStat(ctx, paths...)
				if err != nil {
					failedRsHostsLock.Lock()
					failedRsHosts[host] = struct{}{}
					failedRsHostsLock.Unlock()
					failHostName(host)
					elog.Info("batchStat retry 0", host, err)
					failedRsHostsLock.RLock()
					host = l.nextRsHost(failedRsHosts)
					failedRsHostsLock.RUnlock()
					bucket = l.newBucket(host, "")
					r, err = bucket.BatchStat(ctx, paths...)
					if err != nil {
						failedRsHostsLock.Lock()
						failedRsHosts[host] = struct{}{}
						failedRsHostsLock.Unlock()
						failHostName(host)
						elog.Info("batchStat retry 1", host, err)
						return err
					} else {
						succeedHostName(host)
					}
				} else {
					succeedHostName(host)
				}
				for j, v := range r {
					if v.Code != 200 {
						stats[index+j] = &FileStat{Name: paths[j], Size: -1}
						elog.Warn("stat bad file:", paths[j], "with code:", v.Code)
					} else {
						stats[index+j] = &FileStat{Name: paths[j], Size: v.Data.Fsize}
					}
				}
				return nil
			})
		}(paths[i:i+size], i)
	}
	err := pool.Wait(ctx)
	return stats, err
}

func (l *singleClusterLister) listPrefix(ctx context.Context, prefix string) ([]string, error) {
	failedHosts := make(map[string]struct{})
	rsHost := l.nextRsHost(failedHosts)
	rsfHost := l.nextRsfHost(failedHosts)
	bucket := l.newBucket(rsHost, rsfHost)
	var files []string
	marker := ""
	for {
		r, _, out, err := bucket.List(ctx, prefix, "", marker, 1000)
		if err != nil && err != io.EOF {
			failedHosts[rsfHost] = struct{}{}
			failHostName(rsfHost)
			elog.Info("ListPrefix retry 0", rsfHost, err)
			rsfHost = l.nextRsfHost(failedHosts)
			bucket = l.newBucket(rsHost, rsfHost)
			r, _, out, err = bucket.List(ctx, prefix, "", marker, 1000)
			if err != nil && err != io.EOF {
				failedHosts[rsfHost] = struct{}{}
				failHostName(rsfHost)
				elog.Info("ListPrefix retry 1", rsfHost, err)
				return nil, err
			} else {
				succeedHostName(rsfHost)
			}
		} else {
			succeedHostName(rsfHost)
		}
		elog.Info("list len", marker, len(r))
		for _, v := range r {
			files = append(files, v.Key)
		}

		if out == "" {
			break
		}
		marker = out
	}
	return files, nil
}

func (l *singleClusterLister) newBucket(host, rsfHost string) kodo.Bucket {
	cfg := kodo.Config{
		AccessKey: l.credentials.AccessKey,
		SecretKey: string(l.credentials.SecretKey),
		RSHost:    host,
		RSFHost:   rsfHost,
		UpHosts:   l.upHosts,
	}
	client := kodo.NewWithoutZone(&cfg)
	return client.Bucket(l.bucket)
}

func (l *Lister) batchStab(r io.Reader) []*FileStat {
	j := json.NewDecoder(r)
	var fl []string
	err := j.Decode(&fl)
	if err != nil {
		elog.Error(err)
		return nil
	}
	return l.ListStat(fl)
}
