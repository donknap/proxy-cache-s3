package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/alibaba/higress/plugins/wasm-go/pkg/wrapper"
	"github.com/donknap/proxy-cache-s3/util"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm/types"
	"github.com/tidwall/gjson"
	"net/url"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

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
		originHost string
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
		config.setting.originHost = strings.Replace(value, " ", "", -1)
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
		config.setting.originHost == "" {
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
		parsedURL.RawQuery = parsedURL.Query().Encode()
	case "keep_specified":
		query := url.Values{}
		for _, key := range rule.Keys {
			if parsedURL.Query().Has(key) {
				query.Add(key, parsedURL.Query().Get(key))
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

func getRealSavePath(originPath string) string {
	query := ""
	file := originPath
	if idx := strings.Index(originPath, "?"); idx != -1 {
		query = originPath[idx+1:]
		file = originPath[:idx]
	}

	if query != "" {
		// 计算 MD5
		hash := md5.Sum([]byte(query))
		md5Hash := hex.EncodeToString(hash[:])

		// 分离文件名和扩展名
		ext := filepath.Ext(file)
		base := strings.TrimSuffix(file, ext)

		// 构建新文件名
		return fmt.Sprintf("%s-%s%s", base, md5Hash, ext)
	}

	return originPath
}

func getS3PresignedURL(config W7ProxyCache, path string, method string, expires time.Duration) (string, error) {
	return util.GeneratePresignedURL(
		config.setting.accessKey,
		config.setting.secretKey,
		"",
		config.setting.region,
		config.setting.host,
		config.setting.bucket,
		path,
		method,
		expires,
		"",
	)
}
