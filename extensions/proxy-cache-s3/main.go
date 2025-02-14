package main

import (
	"fmt"
	"github.com/alibaba/higress/plugins/wasm-go/pkg/wrapper"
	"github.com/donknap/proxy-cache-s3/util"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm/types"
	"github.com/tidwall/gjson"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var syncResourceMap = sync.Map{}

func main() {
	wrapper.SetCtx(
		"w7-proxy-cache",
		wrapper.ParseConfigBy(parseConfig),
		wrapper.ProcessRequestHeadersBy(onHttpRequestHeaders),
		wrapper.ProcessResponseHeadersBy(onHttpResponseHeaders),
	)
}

var targetClientMap = sync.Map{}

type pathCacheRule struct {
	CacheType string   `json:"cache_type"`
	Paths     []string `json:"paths"`
	Enable    bool     `json:"enable"`
	CacheTtl  int64    `json:"cache_ttl"`
	Weight    int64    `json:"weight"`
}

type pathKeyCacheRule struct {
	CacheType     string   `json:"cache_type"`
	Paths         []string `json:"paths"`
	IgnoreKeyRule string   `json:"ignore_key_rule"`
	Keys          []string `json:"keys"`
	IgnoreCase    bool     `json:"ignore_case"`
	Weight        int64    `json:"weight"`
}

type W7ProxyCache struct {
	client  wrapper.HttpClient
	setting struct {
		accessKey      string
		secretKey      string
		region         string
		bucket         string
		host           string
		port           int64
		purgeReqMethod string

		pathCacheRules    []pathCacheRule
		pathKeyCacheRules []pathKeyCacheRule

		syncTickStep int64
		syncNum      int64

		targetHost string
	}
}

func parseConfig(data gjson.Result, config *W7ProxyCache, log wrapper.Log) error {
	log.Errorf("parseConfig, %s", data.String())

	if data.Get("access_key").Exists() {
		value := data.Get("access_key").String()
		config.setting.accessKey = strings.Replace(value, " ", "", -1)
	}
	if data.Get("secret_key").Exists() {
		value := data.Get("secret_key").String()
		config.setting.secretKey = strings.Replace(value, " ", "", -1)
	}
	if data.Get("region").Exists() {
		value := data.Get("region").String()
		config.setting.region = strings.Replace(value, " ", "", -1)
	}
	if data.Get("bucket").Exists() {
		value := data.Get("bucket").String()
		config.setting.bucket = strings.Replace(value, " ", "", -1)
	}
	if data.Get("host").Exists() {
		value := data.Get("host").String()
		config.setting.host = strings.Replace(value, " ", "", -1)
	}
	if data.Get("port").Exists() {
		config.setting.port = data.Get("port").Int()
	}
	if data.Get("purge_req_method").Exists() {
		value := data.Get("purge_req_method").String()
		config.setting.purgeReqMethod = strings.Replace(value, " ", "", -1)
	}
	if data.Get("rewrite_host").Exists() {
		value := data.Get("rewrite_host").String()
		config.setting.targetHost = strings.Replace(value, " ", "", -1)
	}
	if data.Get("sync_tick_step").Exists() {
		config.setting.syncTickStep = data.Get("sync_tick_step").Int()
	}
	if data.Get("sync_num").Exists() {
		config.setting.syncNum = data.Get("sync_num").Int()
	}

	if config.setting.accessKey == "" ||
		config.setting.secretKey == "" ||
		config.setting.region == "" ||
		config.setting.bucket == "" ||
		config.setting.host == "" ||
		config.setting.targetHost == "" {
		log.Error("s3 setting is empty")
		return types.ErrorStatusBadArgument
	}

	if config.setting.port == 0 {
		config.setting.port = 80
	}
	if config.setting.syncTickStep == 0 {
		config.setting.syncTickStep = 3000
	}
	if config.setting.syncNum == 0 {
		config.setting.syncNum = 8
	}

	config.setting.pathCacheRules = []pathCacheRule{}
	if data.Get("path_cache_rules").Exists() {
		rulesData := data.Get("path_cache_rules").Array()
		for _, item := range rulesData {
			paths := []string{}
			for _, path := range item.Get("paths").Array() {
				paths = append(paths, path.String())
			}
			config.setting.pathCacheRules = append(config.setting.pathCacheRules, pathCacheRule{
				CacheType: item.Get("cache_type").String(),
				Paths:     paths,
				Enable:    item.Get("enable").Bool(),
				CacheTtl:  item.Get("cache_ttl").Int(),
				Weight:    item.Get("weight").Int(),
			})
		}
		sort.Slice(config.setting.pathCacheRules, func(i, j int) bool {
			return config.setting.pathCacheRules[i].Weight < config.setting.pathCacheRules[j].Weight
		})
		log.Errorf("pathCacheRules: %v", config.setting.pathCacheRules)
	}
	config.setting.pathKeyCacheRules = []pathKeyCacheRule{}
	if data.Get("path_key_cache_rules").Exists() {
		for _, item := range data.Get("path_key_cache_rules").Array() {
			keys := []string{}
			for _, key := range item.Get("keys").Array() {
				keys = append(keys, key.String())
			}
			paths := []string{}
			for _, path := range item.Get("paths").Array() {
				paths = append(paths, path.String())
			}
			config.setting.pathKeyCacheRules = append(config.setting.pathKeyCacheRules, pathKeyCacheRule{
				CacheType:     item.Get("cache_type").String(),
				Paths:         paths,
				IgnoreKeyRule: item.Get("ignore_key_rule").String(),
				Keys:          keys,
				IgnoreCase:    item.Get("ignore_case").Bool(),
				Weight:        item.Get("weight").Int(),
			})
		}
		sort.Slice(config.setting.pathKeyCacheRules, func(i, j int) bool {
			return config.setting.pathKeyCacheRules[i].Weight < config.setting.pathKeyCacheRules[j].Weight
		})
		log.Errorf("pathKeyCacheRules: %v", config.setting.pathKeyCacheRules)
	}

	urlServiceInfo := strings.Replace(config.setting.host, ".svc.cluster.local", "", 1)
	urlServiceInfoArr := strings.Split(urlServiceInfo, ".")
	if len(urlServiceInfoArr) != 2 {
		log.Errorf("invalid host: %s", config.setting.host)
		return types.ErrorStatusBadArgument
	}
	config.client = wrapper.NewClusterClient(wrapper.K8sCluster{
		Port:        config.setting.port,
		Version:     "",
		ServiceName: urlServiceInfoArr[0],
		Namespace:   urlServiceInfoArr[1],
	})

	wrapper.RegisteTickFunc(config.setting.syncTickStep, syncResource(config.setting.syncNum, log))

	return nil
}

func syncResource(syncNum int64, log wrapper.Log) func() {
	return func() {
		curNum := int64(0)

		syncedList := make([]string, 0)
		syncResourceMap.Range(func(key, value any) bool {
			reqPath := key.(string)
			log.Errorf("syncResource: %s", reqPath)

			info, ok := value.(map[string]interface{})
			if !ok {
				log.Errorf("syncResource value type error")
				return true
			}
			config := W7ProxyCache{}
			_, exists := info["config"]
			if !exists {
				log.Errorf("syncResource get config failed: %s", reqPath)
				return true
			} else {
				config = info["config"].(W7ProxyCache)
			}
			clusterName := ""
			_, exists = info["cluster_name"]
			if !exists {
				log.Errorf("syncResource get cluster_name failed: %s", reqPath)
				return true
			} else {
				clusterName = info["cluster_name"].(string)
			}
			log.Errorf("syncResource cluster_name: %s", clusterName)
			var targetClient wrapper.HttpClient
			_targetClient, exists := targetClientMap.Load(clusterName)
			if !exists {
				clusterInfo := strings.Split(clusterName, "|")
				if len(clusterInfo) != 4 {
					log.Errorf("invalid cluster_name: %s", clusterName)
					return true
				}
				port, err := strconv.Atoi(clusterInfo[1])
				if err != nil {
					log.Errorf("invalid port: %s", clusterInfo[1])
					return true
				}

				serviceName := strings.ReplaceAll(clusterInfo[3], ".dns", "")
				targetClient = wrapper.NewClusterClient(wrapper.DnsCluster{
					Port:        int64(port),
					ServiceName: serviceName,
					Domain:      config.setting.targetHost,
				})
				log.Errorf("syncResource get s3 path: %s, %s, %d", config.setting.targetHost, serviceName, port)
				log.Errorf("syncResource get s3 path: %s, %d", config.setting.targetHost, port)
				targetClientMap.Store(clusterName, targetClient)
			} else {
				targetClient = _targetClient.(wrapper.HttpClient)
			}

			err := targetClient.Get(reqPath, nil, func(statusCode int, responseHeaders http.Header, responseBody []byte) {
				log.Errorf("syncResource get complete: %d, %s", statusCode, reqPath)
				if statusCode != 200 {
					return
				}

				putPath, err := util.GeneratePresignedURL(
					config.setting.accessKey,
					config.setting.secretKey,
					"",
					config.setting.region,
					config.setting.host,
					config.setting.bucket,
					reqPath,
					"PUT",
					3600*time.Second,
					"",
				)
				log.Errorf("syncResource put s3 path: %s, %s", reqPath, putPath)
				if err != nil {
					log.Errorf("syncResource make s3 url failed: %v", err)
					return
				}

				headers := make([][2]string, 0)
				headerData, exists := info["headers"]
				if exists {
					headers = headerData.([][2]string)
				}
				fmt.Print("headers: %v", headers)

				err = config.client.Put(putPath, headers, responseBody, func(statusCode int, responseHeaders http.Header, responseBody []byte) {
					log.Errorf("syncResource sync complete: %d, %s", statusCode, reqPath)
				})
				if err != nil {
					log.Errorf("syncResource put s3 failed: %v", err)
				}
			})
			if err != nil {
				log.Errorf("syncResource failed: %v", err)
			}

			syncedList = append(syncedList, reqPath)

			curNum += 1
			if curNum >= syncNum {
				return false
			}

			return true
		})

		for _, item := range syncedList {
			syncResourceMap.Delete(item)
		}
	}
}

func getPathCacheRule(path string, rules []pathCacheRule) (*pathCacheRule, error) {
	parsedURL, err := url.Parse(path)
	if err != nil {
		return nil, err
	}
	path = strings.TrimLeft(parsedURL.Path, "/")
	if rules == nil || len(rules) == 0 {
		return nil, nil
	}

	var defaultRule pathCacheRule
	for _, rule := range rules {
		switch rule.CacheType {
		case "suffix":
			for _, rpath := range rule.Paths {
				if strings.HasSuffix(path, rpath) {
					return &rule, nil
				}
			}
		case "path":
			for _, rpath := range rule.Paths {
				if path == rpath {
					return &rule, nil
				}
			}
		case "dir":
			for _, rpath := range rule.Paths {
				if strings.HasPrefix(path, rpath) {
					return &rule, nil
				}
			}
		case "all":
			defaultRule = rule // 保存匹配所有文件的规则
		default:
			fmt.Printf("Unknown cacheType: %s\n", rule.CacheType)
			return nil, fmt.Errorf("Unknown cacheType: %s", rule.CacheType)
		}
	}

	// 如果没有找到其他匹配规则，返回默认规则
	return &defaultRule, nil
}

func getPathKeyCacheRule(path string, rules []pathKeyCacheRule) (*pathKeyCacheRule, error) {
	parsedURL, err := url.Parse(path)
	if err != nil {
		return nil, err
	}
	path = strings.TrimLeft(parsedURL.Path, "/")
	if rules == nil || len(rules) == 0 {
		return nil, nil
	}

	var defaultRule pathKeyCacheRule
	for _, rule := range rules {
		switch rule.CacheType {
		case "suffix":
			if rule.IgnoreCase {
				for _, rpath := range rule.Paths {
					if strings.HasSuffix(strings.ToLower(path), strings.ToLower(rpath)) {
						return &rule, nil
					}
				}

			} else {
				for _, rpath := range rule.Paths {
					if strings.HasSuffix(path, rpath) {
						return &rule, nil
					}
				}
			}
		case "path":
			if rule.IgnoreCase {
				for _, rpath := range rule.Paths {
					if strings.ToLower(path) == strings.ToLower(rpath) {
						return &rule, nil
					}
				}
			} else {
				for _, rpath := range rule.Paths {
					if path == rpath {
						return &rule, nil
					}
				}
			}
		case "dir":
			if rule.IgnoreCase {
				for _, rpath := range rule.Paths {
					if strings.HasPrefix(strings.ToLower(path), strings.ToLower(rpath)) {
						return &rule, nil
					}
				}
			} else {
				for _, rpath := range rule.Paths {
					if strings.HasPrefix(path, rpath) {
						return &rule, nil
					}
				}
			}
		case "all":
			defaultRule = rule // 保存匹配所有文件的规则
		default:
			fmt.Printf("Unknown cacheType: %s\n", rule.CacheType)
			return nil, fmt.Errorf("Unknown cacheType: %s", rule.CacheType)
		}
	}

	// 如果没有找到其他匹配规则，返回默认规则
	return &defaultRule, nil
}

func processPathByRule(path string, rule *pathKeyCacheRule) string {
	parsedURL, err := url.Parse(path)
	if err != nil {
		return path
	}

	// 根据 IgnoreKeyRule 处理查询参数
	switch rule.IgnoreKeyRule {
	case "ignore":
		parsedURL.RawQuery = ""
	case "keep":
		// 保留所有查询参数
	case "keep_specified":
		query := url.Values{}
		for _, key := range rule.Keys {
			if value := parsedURL.Query().Get(key); value != "" {
				query.Add(key, value)
			}
		}
		parsedURL.RawQuery = query.Encode()
	case "ignore_specified":
		query := url.Values{}
		for key, values := range parsedURL.Query() {
			for _, value := range values {
				query.Add(key, value)
			}
		}
		for _, key := range rule.Keys {
			query.Del(key)
		}
		parsedURL.RawQuery = query.Encode()
	}

	return parsedURL.String()
}

func onHttpRequestHeaders(ctx wrapper.HttpContext, config W7ProxyCache, log wrapper.Log) types.Action {
	if config.setting.purgeReqMethod != "" && strings.ToLower(config.setting.purgeReqMethod) == strings.ToLower(ctx.Method()) {
		return types.ActionContinue
	}

	clusterName, err := proxywasm.GetProperty([]string{"cluster_name"})
	if err != nil {
		log.Errorf("onHttpRequestHeaders get cluster_name failed: %v", err)
		return types.ActionContinue
	}
	ctx.SetContext("cluster_name", string(clusterName))

	//检测是否需要缓存，如果需要缓存，则将请求转发到s3
	_pathCacheRule, err := getPathCacheRule(ctx.Path(), config.setting.pathCacheRules)
	if err != nil {
		log.Errorf("onHttpRequestHeaders get cache rule failed: %v", err)
		ctx.SetContext("cache_enable", false)
		return types.ActionContinue
	}
	if _pathCacheRule == nil {
		ctx.SetContext("cache_enable", false)
		return types.ActionContinue
	}
	log.Errorf("onHttpRequestHeaders get cache rule %v", _pathCacheRule)
	ctx.SetContext("cache_enable", _pathCacheRule.Enable)
	if !_pathCacheRule.Enable {
		return types.ActionContinue
	}

	realPath := ctx.Path()
	_pathKeyCacheRule, err := getPathKeyCacheRule(realPath, config.setting.pathKeyCacheRules)
	if err != nil {
		log.Errorf("onHttpRequestHeaders get cache key rule failed: %v", err)
	}
	if _pathKeyCacheRule != nil {
		realPath = processPathByRule(realPath, _pathKeyCacheRule)
	}

	checkExistsUrl, err := util.GeneratePresignedURL(
		config.setting.accessKey,
		config.setting.secretKey,
		"",
		config.setting.region,
		config.setting.host,
		config.setting.bucket,
		realPath,
		"GET",
		30*time.Second,
		"",
	)
	if err != nil {
		log.Errorf("onHttpRequestHeaders make s3 check url failed: %v", err)
		return types.ActionContinue
	}
	ctx.SetContext("req_path", realPath)

	log.Errorf("onHttpRequestHeaders check s3 path: %s, bucket: %s", realPath, config.setting.bucket)
	err = config.client.Get(checkExistsUrl, nil, func(statusCode int, responseHeaders http.Header, responseBody []byte) {
		exists := false
		if statusCode == 200 {
			exists = true
		}
		modifiedAt := responseHeaders.Get("last-modified")
		if modifiedAt == "" {
			exists = false
		}
		log.Errorf("onHttpRequestHeaders check s3 complete: %s, %d, %s", realPath, statusCode, modifiedAt)

		if exists && _pathCacheRule.CacheTtl > 0 {
			datetime, err := time.Parse(time.RFC1123, modifiedAt)
			if err == nil {
				// 计算从datetime到现在的时间差（秒）
				duration := time.Since(datetime).Seconds()
				if duration > float64(_pathCacheRule.CacheTtl) {
					exists = false
				}
			} else {
				exists = false
			}
		}
		if exists {
			ctx.SetContext("s3_file_exists", true)
			log.Errorf("onHttpRequestHeaders s3 file exists: %s", realPath)

			headers := make([][2]string, 0)
			for key, item := range responseHeaders {
				headers = append(headers, [2]string{key, item[0]})
			}
			err = proxywasm.SendHttpResponse(uint32(statusCode), headers, responseBody, -1)
			if err != nil {
				log.Errorf("onHttpRequestHeaders send response failed %s", err.Error())
			}

			return
		}

		log.Errorf("onHttpRequestHeaders check s3 complete1: %s, %d, %s", realPath, statusCode, modifiedAt)

		err = proxywasm.ResumeHttpRequest()
		if err != nil {
			log.Errorf("onHttpRequestHeaders resume request failed %s", err.Error())
			return
		}
	}, 30000)
	if err != nil {
		return types.ActionContinue
	}

	return types.ActionPause
}

func onHttpResponseHeaders(ctx wrapper.HttpContext, config W7ProxyCache, log wrapper.Log) types.Action {
	enableCache := ctx.GetBoolContext("cache_enable", false)
	if !enableCache {
		return types.ActionContinue
	}

	reqPath := ctx.GetStringContext("req_path", "")

	log.Errorf("onHttpResponseHeaders begin %s", reqPath)
	status, err := proxywasm.GetHttpResponseHeader(":status")
	if err != nil {
		log.Errorf("onHttpResponseHeaders get status failed %s", err.Error())
		return types.ActionContinue
	}
	if status == "200" {
		contentType, err := proxywasm.GetHttpResponseHeader("content-type")
		if err != nil {
			log.Errorf("onHttpResponseHeaders get content type failed %s", err.Error())
			return types.ActionContinue
		}

		s3FileExists := ctx.GetBoolContext("s3_file_exists", false)
		if !s3FileExists {
			headers := make([][2]string, 0)
			if contentType != "" {
				headers = append(headers, [2]string{"Content-Type", contentType})
			}

			log.Errorf("onHttpResponseHeaders sync complete %s", reqPath)

			syncResourceMap.Store(reqPath, map[string]interface{}{
				"headers":      headers,
				"cluster_name": ctx.GetStringContext("cluster_name", ""),
				"config":       config,
			})
		}
	}

	return types.ActionContinue
}
