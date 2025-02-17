package main

import (
	"fmt"
	"github.com/alibaba/higress/plugins/wasm-go/pkg/wrapper"
	"github.com/donknap/proxy-cache-s3/util"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm/types"
	"github.com/tidwall/gjson"
	"net/http"
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
		cacheTTL       int64

		syncTickStep int64
		syncNum      int64

		targetHost string
	}
}

func parseConfig(json gjson.Result, config *W7ProxyCache, log wrapper.Log) error {
	log.Errorf("parseConfig, %s", json.String())

	// get cache ttl
	if json.Get("cache_ttl").Exists() {
		cacheTTL := json.Get("cache_ttl").Int()
		config.setting.cacheTTL = cacheTTL
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
	if json.Get("port").Exists() {
		config.setting.port = json.Get("port").Int()
	}
	if json.Get("purge_req_method").Exists() {
		value := json.Get("purge_req_method").String()
		config.setting.purgeReqMethod = strings.Replace(value, " ", "", -1)
	}
	if json.Get("rewrite_host").Exists() {
		value := json.Get("rewrite_host").String()
		config.setting.targetHost = strings.Replace(value, " ", "", -1)
	}
	if json.Get("sync_tick_step").Exists() {
		config.setting.syncTickStep = json.Get("sync_tick_step").Int()
	}
	if json.Get("sync_num").Exists() {
		config.setting.syncNum = json.Get("sync_num").Int()
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

	checkExistsUrl, err := util.GeneratePresignedURL(
		config.setting.accessKey,
		config.setting.secretKey,
		"",
		config.setting.region,
		config.setting.host,
		config.setting.bucket,
		ctx.Path(),
		"GET",
		30*time.Second,
		"",
	)
	if err != nil {
		log.Errorf("onHttpRequestHeaders make s3 check url failed: %v", err)
		return types.ActionContinue
	}
	ctx.SetContext("req_path", ctx.Path())

	log.Errorf("onHttpRequestHeaders check s3 path: %s, bucket: %s", ctx.Path(), config.setting.bucket)
	err = config.client.Get(checkExistsUrl, nil, func(statusCode int, responseHeaders http.Header, responseBody []byte) {
		exists := false
		if statusCode == 200 {
			exists = true
		}
		modifiedAt := responseHeaders.Get("last-modified")
		if modifiedAt == "" {
			exists = false
		}
		log.Errorf("onHttpRequestHeaders check s3 complete: %s, %d, %s", ctx.Path(), statusCode, modifiedAt)

		if exists && config.setting.cacheTTL > 0 {
			datetime, err := time.Parse(time.RFC1123, modifiedAt)
			if err == nil {
				// 计算从datetime到现在的时间差（秒）
				duration := time.Since(datetime).Seconds()
				if duration > float64(config.setting.cacheTTL) {
					exists = false
				}
			} else {
				exists = false
			}
		}
		if exists {
			ctx.SetContext("s3_file_exists", true)
			log.Errorf("onHttpRequestHeaders s3 file exists: %s", ctx.Path())

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

		log.Errorf("onHttpRequestHeaders check s3 complete1: %s, %d, %s", ctx.Path(), statusCode, modifiedAt)

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
