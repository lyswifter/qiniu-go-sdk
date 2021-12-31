package operation

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qiniupd/qiniu-go-sdk/api.v7/auth/qbox"
	"github.com/qiniupd/qiniu-go-sdk/x/rpc.v7"
)

var downloadClient = &http.Client{
	Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   1 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	},
	Timeout: 10 * time.Minute,
}

// 下载器
type Downloader struct {
	config                   Configurable
	singleClusterDownloader  *singleClusterDownloader
	multiClustersConcurrency int
}

// 根据配置创建下载器
func NewDownloader(c *Config) *Downloader {
	return &Downloader{config: c, singleClusterDownloader: newSingleClusterDownloader(c)}
}

// 根据环境变量创建下载器
func NewDownloaderV2() *Downloader {
	c := getCurrentConfigurable()
	if c == nil {
		return nil
	} else if singleClusterConfig, ok := c.(*Config); ok {
		return NewDownloader(singleClusterConfig)
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
		return &Downloader{config: c, multiClustersConcurrency: concurrency}
	}
}

// 检查文件
func (d *Downloader) DownloadCheck(key string) (l int64, err error) {
	l, _, err = d.DownloadRangeBytes(key, -1, 4)
	return
}

func (d *Downloader) DownloadCheckList(ctx context.Context, keys []string) ([]*FileStat, error) {
	if d.singleClusterDownloader != nil {
		return d.singleClusterDownloader.downloadCheckList(ctx, keys)
	}

	type KeysWithIndex struct {
		IndexMap []int
		Keys     []string
	}

	clusterPathsMap := make(map[*Config]*KeysWithIndex)
	for i, key := range keys {
		config, exists := d.config.forKey(key)
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

	pool := newGoroutinePool(d.multiClustersConcurrency)
	allStats := make([]*FileStat, len(keys))
	for config, keysWithIndex := range clusterPathsMap {
		func(config *Config, keys []string, indexMap []int) {
			pool.Go(func(ctx context.Context) error {
				ds := newSingleClusterDownloader(config)
				if stats, err := ds.downloadCheckList(ctx, keys); err != nil {
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
	err := pool.Wait(ctx)
	return allStats, err
}

// 下载指定对象到文件里
func (d *Downloader) DownloadFile(key, path string) (f *os.File, err error) {
	if d.singleClusterDownloader != nil {
		return d.singleClusterDownloader.downloadFile(key, path)
	}
	if config, exists := d.config.forKey(key); !exists {
		return nil, ErrUndefinedConfig
	} else {
		return newSingleClusterDownloader(config).downloadFile(key, path)
	}
}

// 下载指定对象到文件里
func (d *Downloader) DownloadBytes(key string) (data []byte, err error) {
	if d.singleClusterDownloader != nil {
		return d.singleClusterDownloader.downloadBytes(key)
	}
	if config, exists := d.config.forKey(key); !exists {
		return nil, ErrUndefinedConfig
	} else {
		return newSingleClusterDownloader(config).downloadBytes(key)
	}
}

// 下载指定对象的指定范围到内存中
func (d *Downloader) DownloadRangeBytes(key string, offset, size int64) (l int64, data []byte, err error) {
	if d.singleClusterDownloader != nil {
		return d.singleClusterDownloader.downloadRangeBytes(key, offset, size)
	}
	if config, exists := d.config.forKey(key); !exists {
		return 0, nil, ErrUndefinedConfig
	} else {
		return newSingleClusterDownloader(config).downloadRangeBytes(key, offset, size)
	}
}

type singleClusterDownloader struct {
	bucket      string
	ioHosts     []string
	credentials *qbox.Mac
	queryer     *Queryer
}

func newSingleClusterDownloader(c *Config) *singleClusterDownloader {
	mac := qbox.NewMac(c.Ak, c.Sk)

	var queryer *Queryer = nil

	if len(c.UcHosts) > 0 {
		queryer = NewQueryer(c)
	}

	downloader := singleClusterDownloader{
		bucket:      c.Bucket,
		ioHosts:     dupStrings(c.IoHosts),
		credentials: mac,
		queryer:     queryer,
	}
	shuffleHosts(downloader.ioHosts)
	return &downloader
}

func (d *singleClusterDownloader) downloadFile(key, path string) (f *os.File, err error) {
	failedIoHosts := make(map[string]struct{})
	for i := 0; i < 3; i++ {
		f, err = d.downloadFileInner(key, path, failedIoHosts)
		if err == nil {
			return
		}
	}
	return
}

func (d *singleClusterDownloader) downloadBytes(key string) (data []byte, err error) {
	failedIoHosts := make(map[string]struct{})
	for i := 0; i < 3; i++ {
		data, err = d.downloadBytesInner(key, failedIoHosts)
		if err == nil {
			break
		}
	}
	return
}

func (d *singleClusterDownloader) downloadRangeBytes(key string, offset, size int64) (l int64, data []byte, err error) {
	failedIoHosts := make(map[string]struct{})
	for i := 0; i < 3; i++ {
		l, data, err = d.downloadRangeBytesInner(key, offset, size, failedIoHosts)
		if err == nil {
			break
		}
	}
	return
}

func (d *singleClusterDownloader) downloadCheckList(ctx context.Context, keys []string) ([]*FileStat, error) {
	concurrency := runtime.NumCPU()
	var (
		length                  = len(keys)
		stats                   = make([]*FileStat, length)
		index             int32 = 0
		failedIoHosts           = make(map[string]struct{})
		failedIoHostsLock sync.RWMutex
		pool              = newGoroutinePool(concurrency)
	)

	for i := 0; i < concurrency; i++ {
		pool.Go(func(ctx context.Context) error {
			for {
				pos := int(atomic.AddInt32(&index, 1) - 1)
				if pos >= length {
					return nil
				}
				for j := 0; j < 3; j++ {
					failedIoHostsLock.RLock()
					host := d.nextHost(failedIoHosts)
					failedIoHostsLock.RUnlock()
					k := keys[pos]
					stat, err := d.downloadCheckInner(k, host)
					stats[pos] = stat
					if err == nil {
						succeedHostName(host)
						break
					}
					if stat.code == PathError {
						elog.Info("path stat error ", k, err)
						break
					}

					if stat.code == HostError {
						elog.Info("stat retry ", k, j, host, err)
						failedIoHostsLock.RLock()
						failedIoHosts[host] = struct{}{}
						failedIoHostsLock.RUnlock()
						failHostName(host)
						continue
					}
				}
			}
		})
	}
	pool.Wait(ctx)
	return stats, nil
}

var curIoHostIndex uint32 = 0

func (d *singleClusterDownloader) nextHost(failedHosts map[string]struct{}) string {
	ioHosts := d.ioHosts
	if d.queryer != nil {
		if hosts := d.queryer.QueryIoHosts(false); len(hosts) > 0 {
			shuffleHosts(hosts)
			ioHosts = hosts
		}
	}
	switch len(ioHosts) {
	case 0:
		panic("No Io hosts is configured")
	case 1:
		return ioHosts[0]
	default:
		var ioHost string
		for i := 0; i <= len(ioHosts)*MaxFindHostsPrecent/100; i++ {
			index := int(atomic.AddUint32(&curIoHostIndex, 1) - 1)
			ioHost = ioHosts[index%len(ioHosts)]
			if _, isFailedBefore := failedHosts[ioHost]; !isFailedBefore && isHostNameValid(ioHost) {
				break
			}
		}
		return ioHost
	}
}

func (d *singleClusterDownloader) downloadFileInner(key, path string, failedIoHosts map[string]struct{}) (*os.File, error) {
	key = strings.TrimPrefix(key, "/")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	length, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	host := d.nextHost(failedIoHosts)

	fmt.Println("remote path", key)
	url := fmt.Sprintf("%s/getfile/%s/%s/%s", host, d.credentials.AccessKey, d.bucket, key)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		failedIoHosts[host] = struct{}{}
		failHostName(host)
		return nil, err
	}
	req.Header.Set("Accept-Encoding", "")
	req.Header.Set("User-Agent", rpc.UserAgent)
	if length != 0 {
		r := fmt.Sprintf("bytes=%d-", length)
		req.Header.Set("Range", r)
		fmt.Println("continue download")
	}

	response, err := downloadClient.Do(req)
	if err != nil {
		failedIoHosts[host] = struct{}{}
		failHostName(host)
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		succeedHostName(host)
		return f, nil
	}
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusPartialContent {
		failedIoHosts[host] = struct{}{}
		failHostName(host)
		return nil, errors.New(response.Status)
	}
	succeedHostName(host)
	ctLength := response.ContentLength
	n, err := io.Copy(f, response.Body)
	if err != nil {
		return nil, err
	}
	if ctLength != n {
		elog.Warn("download length not equal", ctLength, n)
	}
	f.Seek(0, io.SeekStart)
	return f, nil
}

func (d *singleClusterDownloader) downloadBytesInner(key string, failedIoHosts map[string]struct{}) ([]byte, error) {
	key = strings.TrimPrefix(key, "/")
	host := d.nextHost(failedIoHosts)

	url := fmt.Sprintf("%s/getfile/%s/%s/%s", host, d.credentials.AccessKey, d.bucket, key)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", rpc.UserAgent)
	response, err := downloadClient.Do(req)
	if err != nil {
		failedIoHosts[host] = struct{}{}
		failHostName(host)
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		failedIoHosts[host] = struct{}{}
		failHostName(host)
		return nil, errors.New(response.Status)
	}
	succeedHostName(host)
	return ioutil.ReadAll(response.Body)
}

func generateRange(offset, size int64) string {
	if offset == -1 {
		return fmt.Sprintf("bytes=-%d", size)
	}
	return fmt.Sprintf("bytes=%d-%d", offset, offset+size)
}

const PathError = -1
const HostError = -2
const FileError = -3

func (d *singleClusterDownloader) downloadCheckInner(key, host string) (f *FileStat, err error) {
	key = strings.TrimPrefix(key, "/")

	url := fmt.Sprintf("%s/getfile/%s/%s/%s", host, d.credentials.AccessKey, d.bucket, key)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return &FileStat{
			Name: key,
			Size: -1,
			code: PathError,
		}, err
	}

	req.Header.Set("Range", generateRange(-1, 4))
	req.Header.Set("User-Agent", rpc.UserAgent)
	response, err := downloadClient.Do(req)
	if err != nil {
		return &FileStat{
			Name: key,
			Size: -1,
			code: HostError,
		}, err
	}
	defer response.Body.Close()
	if response.StatusCode >= 500 {
		return &FileStat{
			Name: key,
			Size: 0,
			code: HostError,
		}, errors.New(response.Status)
	}

	if response.StatusCode == http.StatusNotFound {
		return &FileStat{
			Name: key,
			Size: -1,
			code: FileError,
		}, os.ErrNotExist
	}

	if response.StatusCode != http.StatusPartialContent {
		return &FileStat{
			Name: key,
			Size: 0,
			code: HostError,
		}, errors.New(response.Status)
	}

	rangeResponse := response.Header.Get("Content-Range")
	if rangeResponse == "" {
		return &FileStat{
			Name: key,
			Size: 0,
			code: HostError,
		}, errors.New("no content range")
	}

	l, err := getTotalLength(rangeResponse)
	if err != nil {
		return &FileStat{
			Name: key,
			Size: 0,
			code: HostError,
		}, err
	}
	_, err = ioutil.ReadAll(response.Body)
	if err != nil {
		return &FileStat{
			Name: key,
			Size: l,
			code: HostError,
		}, err
	}
	return &FileStat{
		Name: key,
		Size: l,
		code: 0,
	}, nil
}

func (d *singleClusterDownloader) downloadRangeBytesInner(key string, offset, size int64, failedIoHosts map[string]struct{}) (int64, []byte, error) {
	key = strings.TrimPrefix(key, "/")
	host := d.nextHost(failedIoHosts)

	url := fmt.Sprintf("%s/getfile/%s/%s/%s", host, d.credentials.AccessKey, d.bucket, key)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		failedIoHosts[host] = struct{}{}
		failHostName(host)
		return -1, nil, err
	}

	req.Header.Set("Range", generateRange(offset, size))
	req.Header.Set("User-Agent", rpc.UserAgent)
	response, err := downloadClient.Do(req)
	if err != nil {
		failedIoHosts[host] = struct{}{}
		failHostName(host)
		return -1, nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusPartialContent {
		failedIoHosts[host] = struct{}{}
		failHostName(host)
		return -1, nil, errors.New(response.Status)
	}

	rangeResponse := response.Header.Get("Content-Range")
	if rangeResponse == "" {
		failedIoHosts[host] = struct{}{}
		failHostName(host)
		return -1, nil, errors.New("no content range")
	}

	l, err := getTotalLength(rangeResponse)
	if err != nil {
		failedIoHosts[host] = struct{}{}
		failHostName(host)
		return -1, nil, err
	}
	b, err := ioutil.ReadAll(response.Body)
	if err != nil {
		failedIoHosts[host] = struct{}{}
		failHostName(host)
	} else {
		succeedHostName(host)
	}
	return l, b, err
}

func getTotalLength(crange string) (int64, error) {
	cr := strings.Split(crange, "/")
	if len(cr) != 2 {
		return -1, errors.New("wrong range " + crange)
	}

	return strconv.ParseInt(cr[1], 10, 64)
}
