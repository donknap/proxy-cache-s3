package main

import (
	"github.com/alibaba/higress/plugins/wasm-go/pkg/wrapper"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm/types"
	"net/http"
	"net/url"
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

			originClient, err := getOriginClient(clusterName, config.setting.originHost)
			if err != nil {
				log.Errorf("syncResource get origin client failed: %s", reqPath)
				return true
			}
			err = originClient.Get(reqPath, nil, func(statusCode int, responseHeaders http.Header, responseBody []byte) {
				log.Errorf("syncResource origin resource get complete: %d, %s", statusCode, reqPath)
				if statusCode != 200 {
					return
				}

				s3SavePath := getRealSavePath(reqPath)
				putPath, err := getS3PresignedURL(config, s3SavePath, "PUT", 3600*time.Second)
				if err != nil {
					log.Errorf("syncResource make s3 url failed: %v", err)
					return
				}
				log.Errorf("syncResource put s3 path: %s, %s, %s", reqPath, s3SavePath, putPath)

				headers := make([][2]string, 0)
				headerData, exists := info["headers"]
				if exists {
					headers = headerData.([][2]string)
				}
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
	reqPath := ctx.Path()

	clusterName, err := proxywasm.GetProperty([]string{"cluster_name"})
	if err != nil {
		log.Errorf("onHttpRequestHeaders get cluster_name failed: %v", err)
		return types.ActionContinue
	}
	ctx.SetContext("cluster_name", string(clusterName))

	//检测是否需要缓存，如果需要缓存，则将请求转发到s3
	_pathCacheRule, err := getPathCacheRule(reqPath, config.setting.pathCacheRules)
	if err != nil || _pathCacheRule == nil {
		log.Errorf("onHttpRequestHeaders get cache rule failed: %v", err)
		ctx.SetContext("cache_enable", false)
		return types.ActionContinue
	}
	log.Errorf("onHttpRequestHeaders get cache rule %s, %v, %v", reqPath, _pathCacheRule, config.setting.pathCacheRules)
	ctx.SetContext("cache_enable", _pathCacheRule.Enable)
	if !_pathCacheRule.Enable {
		return types.ActionContinue
	}

	//根据规则重置请求地址
	_pathKeyCacheRule, err := getPathKeyCacheRule(reqPath, config.setting.pathKeyCacheRules)
	if err != nil {
		log.Errorf("onHttpRequestHeaders get cache key rule failed: %v", err)
	}
	reqPathProcessPath := reqPath
	if _pathKeyCacheRule != nil {
		reqPathProcessPath = processPathByRule(reqPathProcessPath, _pathKeyCacheRule)
	}
	log.Errorf("onHttpRequestHeaders get cache key rule%s, %s, %v, %v", ctx.Path(), reqPathProcessPath, _pathKeyCacheRule, config.setting.pathKeyCacheRules)

	ctx.SetContext("req_path", reqPathProcessPath)

	s3SavePath := getRealSavePath(reqPathProcessPath)
	checkS3PresignPath, err := getS3PresignedURL(config, s3SavePath, "GET", 30*time.Second)
	if err != nil {
		log.Errorf("onHttpRequestHeaders make s3 check url failed: %v", err)
		return types.ActionContinue
	}
	log.Errorf("onHttpRequestHeaders check s3 path: %s, %s, bucket: %s", reqPathProcessPath, s3SavePath, config.setting.bucket)
	err = config.client.Get(checkS3PresignPath, nil, func(statusCode int, responseHeaders http.Header, responseBody []byte) {
		exists := false
		if statusCode == 200 {
			exists = true
		}
		modifiedAt := responseHeaders.Get("last-modified")
		if modifiedAt == "" {
			exists = false
		}
		log.Errorf("onHttpRequestHeaders check s3 complete: %s, %d, %s", reqPathProcessPath, statusCode, modifiedAt)

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
			log.Errorf("onHttpRequestHeaders s3 file exists: %s", reqPathProcessPath)
			hs, _ := proxywasm.GetHttpRequestHeaders()
			log.Errorf("onHttpResponseHeaders1 begin %s, %v", reqPath, hs)

			reqHeaders := GetOriginalRequestHeaders()
			getS3PresignPath, _ := getS3PresignedURL(config, s3SavePath, "GET", 360*time.Second)
			u, err := url.Parse(getS3PresignPath)
			if err == nil {
				OverwriteRequestHostHeader(reqHeaders, u.Host)
				OverwriteRequestPathHeader(reqHeaders, u.RequestURI())
			}

			ReplaceRequestHeaders(reqHeaders)

			hs, _ = proxywasm.GetHttpRequestHeaders()
			log.Errorf("onHttpResponseHeaders begin %s, %v", reqPath, hs)

			//responseS3Resource(statusCode, responseHeaders, responseBody, log)
			//return
		} else if statusCode == 200 {
			//检测源中是否存在，如果不存在忽略缓存策略，直接返回 s3的资源
			originClient, err := getOriginClient(string(clusterName), config.setting.originHost)
			if err != nil {
				log.Errorf("onHttpRequestHeaders get origin client failed: %s, %v", reqPathProcessPath, err)

				_ = proxywasm.ResumeHttpRequest()
				return
			}

			err = originClient.Head(reqPathProcessPath, nil, func(originStatusCode int, originResponseHeaders http.Header, originResponseBody []byte) {
				log.Errorf("onHttpRequestHeaders s3 origin check: %s, %d, %s", reqPathProcessPath, originStatusCode)
				if originStatusCode != 200 {
					ctx.SetContext("s3_file_exists", true)

					responseS3Resource(statusCode, responseHeaders, responseBody, log)
					return
				}

				_ = proxywasm.ResumeHttpRequest()
			})
			if err != nil {
				_ = proxywasm.ResumeHttpRequest()
				return
			}
			return
		}
		_ = proxywasm.ResumeHttpRequest()

	}, 30000)
	if err != nil {
		log.Errorf("onHttpRequestHeaders check s3 err: %s, %v", reqPathProcessPath, err)
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

	status, err := proxywasm.GetHttpResponseHeader(":status")
	headers, _ := proxywasm.GetHttpResponseHeaders()
	log.Errorf("onHttpResponseHeaders begin %s, %s, %v", reqPath, status, headers)
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

			syncResourceMap.Store(reqPath, map[string]interface{}{
				"headers":      headers,
				"cluster_name": ctx.GetStringContext("cluster_name", ""),
				"config":       config,
			})
			log.Errorf("onHttpResponseHeaders sync push %s", reqPath)
		}
	}

	return types.ActionContinue
}
