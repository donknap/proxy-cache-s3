package main

import (
	"github.com/alibaba/higress/plugins/wasm-go/pkg/wrapper"
	"github.com/donknap/proxy-cache-s3/util"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm/types"
	"github.com/tidwall/gjson"
	"strings"
	"time"
)

const (
	ConfigMethodPURGE = "PURGE" // 主动清理缓存

	CacheKeyHost   = "$host"
	CacheKeyPath   = "$path"
	CacheKeyMethod = "$method"
	CacheKeyCookie = "$cookie"

	CacheHttpStatusCodeOk = 200

	DefaultCacheTTL = "300s"
)

func main() {
	wrapper.SetCtx(
		"w7-proxy-cache",
		wrapper.ParseConfigBy(parseConfig),
		wrapper.ProcessRequestHeadersBy(onHttpRequestHeaders),
		wrapper.ProcessResponseBodyBy(onHttpResponseBody),
	)
}

type W7ProxyCache struct {
	client   wrapper.HttpClient
	cacheKey []string
	setting  struct {
		cacheTTL    string
		cacheHeader bool
		accessKey   string
		secretKey   string
		region      string
		bucket      string
		host        string
	}
}

func parseConfig(json gjson.Result, config *W7ProxyCache, log wrapper.Log) error {
	// create default config
	config.cacheKey = []string{CacheKeyHost, CacheKeyPath, CacheKeyMethod}

	config.client = wrapper.NewClusterClient(wrapper.FQDNCluster{
		FQDN: "proxy-cache-s3-httpbin-1",
		Port: 80,
	})

	// get cache ttl
	if json.Get("cache_ttl").Exists() {
		cacheTTL := json.Get("cache_ttl").String()
		cacheTTL = strings.Replace(cacheTTL, " ", "", -1)
		config.setting.cacheTTL = cacheTTL
	}

	if json.Get("cache_header").Exists() {
		value := json.Get("cache_header").Bool()
		config.setting.cacheHeader = value
		if config.setting.cacheHeader {
			config.cacheKey = append(config.cacheKey, CacheKeyCookie)
		}
	}

	if json.Get("access_key").Exists() {
		value := json.Get("access_key").String()
		config.setting.accessKey = strings.Replace(value, " ", "", -1)
	}

	if json.Get("secret_key").Exists() {
		value := json.Get("secret_key").String()
		config.setting.secretKey = strings.Replace(value, " ", "", -1)
	}

	if json.Get("region").Exists() {
		value := json.Get("region").String()
		config.setting.region = strings.Replace(value, " ", "", -1)
	}

	if json.Get("bucket").Exists() {
		value := json.Get("bucket").String()
		config.setting.bucket = strings.Replace(value, " ", "", -1)
	}

	if json.Get("host").Exists() {
		value := json.Get("host").String()
		config.setting.host = strings.Replace(value, " ", "", -1)
	}

	if config.setting.cacheTTL == "" {
		log.Error("cache ttl is empty")
		return types.ErrorStatusBadArgument
	}

	if config.setting.accessKey == "" ||
		config.setting.secretKey == "" ||
		config.setting.region == "" ||
		config.setting.bucket == "" ||
		config.setting.host == "" {
		log.Error("s3 setting is empty")
		return types.ErrorStatusBadArgument
	}
	log.Info("cachekey: " + strings.Join(config.cacheKey, "==="))

	return nil
}

func onHttpRequestHeaders(ctx wrapper.HttpContext, config W7ProxyCache, log wrapper.Log) types.Action {
	cacheKeyList := make([]string, 0)
	for _, cacheKey := range config.cacheKey {
		switch cacheKey {
		case CacheKeyHost:
			host := ctx.Host()
			cacheKey = CacheKeyHost + host
		case CacheKeyPath:
			path := ctx.Path()
			cacheKey = CacheKeyPath + path
		case CacheKeyMethod:
			method := ctx.Method()
			cacheKey = CacheKeyMethod + method
		case CacheKeyCookie:
			cookie, err := proxywasm.GetHttpRequestHeader("cookie")
			if err != nil {
				log.Error("parse request cookie failed")
				return types.ActionContinue
			}
			cacheKey = CacheKeyCookie + cookie
		default:
			log.Errorf("invalid cache key: %s", cacheKey)
		}
		cacheKeyList = append(cacheKeyList, cacheKey)
	}
	cacheKey := util.GetCacheKey(strings.Join(cacheKeyList, "-"))
	log.Errorf("request cache key: %s", cacheKey)
	return types.ActionContinue
}

func onHttpResponseBody(ctx wrapper.HttpContext, config W7ProxyCache, body []byte, log wrapper.Log) types.Action {
	//status, err := proxywasm.GetHttpResponseHeader("status")
	//log.Infof("response status %s", status)
	//if err != nil {
	//	log.Errorf("parse response status code failed %s", err.Error())
	//	return types.ActionContinue
	//}
	//// convert status code to uint32
	//statusCode, err := strconv.Atoi(status)
	//if err != nil {
	//	log.Errorf("convert status code to uint32 failed: %v", err)
	//	return types.ActionContinue
	//}
	//if !util.InArray([]uint32{
	//	200,
	//}, uint32(statusCode)) {
	//	return types.ActionContinue
	//}
	//// if request method is not GET or HEAD, do not cache it
	//if !util.InArray([]string{
	//	"GET", "HEAD",
	//}, ctx.Method()) {
	//	return types.ActionContinue
	//}
	//// check actual cache key
	//if len(config.cacheKey) == 0 {
	//	log.Error("actual cache key is empty")
	//	return types.ActionContinue
	//}
	log.Error(string(body))

	url, err := util.GeneratePresignedURL(
		config.setting.accessKey,
		config.setting.secretKey,
		"",
		config.setting.region,
		config.setting.host,
		config.setting.bucket,
		util.GetPath(config.cacheKey),
		"put",
		1*time.Hour,
		"",
	)
	if err != nil {
		log.Errorf("make s3 url failed: %v", err)
		return types.ActionContinue
	}
	log.Infof("put s3 path: %s", url)
	//err = config.client.Put(url, nil, body,
	//	func(statusCode int, responseHeaders http.Header, responseBody []byte) {
	//		if statusCode != http.StatusOK {
	//			log.Errorf("put s3 error: %d %s", statusCode, url)
	//		} else {
	//			log.Infof("put s3 success: %d, response body: %s", statusCode, responseBody)
	//		}
	//		proxywasm.ResumeHttpRequest()
	//	},
	//)
	//if err != nil {
	//	log.Errorf("cache response body failed: %v", err)
	//	return types.ActionContinue
	//}
	return types.ActionContinue
}
