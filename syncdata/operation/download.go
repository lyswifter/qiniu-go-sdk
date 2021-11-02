package operation

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
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
	config                  Configurable
	singleClusterDownloader *singleClusterDownloader
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
		return &Downloader{config: c}
	}
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
