package main

import (
	"fmt"
	"github.com/alibaba/higress/plugins/wasm-go/pkg/wrapper"
	"github.com/donknap/proxy-cache-s3/util"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm/types"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

var syncResourceMap = sync.Map{}
var targetClientMap = sync.Map{}

func main() {
	wrapper.SetCtx(
		"w7-proxy-cache",
		wrapper.ParseConfigBy(parseConfig),
		wrapper.ProcessRequestHeadersBy(onHttpRequestHeaders),
		wrapper.ProcessResponseHeadersBy(onHttpResponseHeaders),
	)
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

				realPath := getRealSavePath(reqPath)
				putPath, err := util.GeneratePresignedURL(
					config.setting.accessKey,
					config.setting.secretKey,
					"",
					config.setting.region,
					config.setting.host,
					config.setting.bucket,
					realPath,
					"PUT",
					3600*time.Second,
					"",
				)
				log.Errorf("syncResource put s3 path: %s, %s, %s", reqPath, realPath, putPath)
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
	log.Errorf("onHttpRequestHeaders get cache rule %s, %v, %v", ctx.Path(), _pathCacheRule, config.setting.pathCacheRules)
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
	checkS3Path := getRealSavePath(realPath)
	log.Errorf("onHttpRequestHeaders12 get cache key rule%s, %s, %v, %v", ctx.Path(), checkS3Path, _pathKeyCacheRule, config.setting.pathKeyCacheRules)

	checkExistsUrl, err := util.GeneratePresignedURL(
		config.setting.accessKey,
		config.setting.secretKey,
		"",
		config.setting.region,
		config.setting.host,
		config.setting.bucket,
		checkS3Path,
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
				// 计算从datetime到现在的时间差（分钟）
				duration := time.Since(datetime).Minutes()
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
		log.Errorf("onHttpRequestHeaders check s3 err: %s, %v", realPath, err)
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
