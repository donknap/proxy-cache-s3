package main

import (
	"fmt"
	"github.com/alibaba/higress/plugins/wasm-go/pkg/wrapper"
	"github.com/donknap/proxy-cache-s3/util"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm/types"
	"github.com/tidwall/gjson"
	"net/http"
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
		wrapper.ProcessResponseBodyBy(onHttpResponseBody),
	)
}

type W7ProxyCache struct {
	client       wrapper.HttpClient
	targetClient wrapper.HttpClient
	setting      struct {
		accessKey      string
		secretKey      string
		region         string
		bucket         string
		host           string
		port           int64
		purgeReqMethod string
		cacheTTL       int64

		targetServerName   string
		targetServerDomain string
		targetServerPort   int64

		syncTickStep int64
		syncNum      int64
	}
}

func parseConfig(json gjson.Result, config *W7ProxyCache, log wrapper.Log) error {
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

	if json.Get("target_server_name").Exists() {
		value := json.Get("target_server_name").String()
		config.setting.targetServerName = strings.Replace(value, " ", "", -1)
	}
	if json.Get("target_server_domain").Exists() {
		value := json.Get("target_server_domain").String()
		config.setting.targetServerDomain = strings.Replace(value, " ", "", -1)
	}
	if json.Get("target_server_port").Exists() {
		value := json.Get("target_server_port").Int()
		config.setting.targetServerPort = value
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
		config.setting.targetServerName == "" ||
		config.setting.targetServerDomain == "" {
		log.Error("s3 setting is empty")
		return types.ErrorStatusBadArgument
	}

	if config.setting.port == 0 {
		config.setting.port = 80
	}
	if config.setting.targetServerPort == 0 {
		config.setting.targetServerPort = 80
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

	config.targetClient = wrapper.NewClusterClient(wrapper.DnsCluster{
		Port:        config.setting.targetServerPort,
		ServiceName: config.setting.targetServerName,
		Domain:      config.setting.targetServerDomain,
	})

	wrapper.RegisteTickFunc(config.setting.syncTickStep, syncResource(*config, log))

	return nil
}

func syncResource(config W7ProxyCache, log wrapper.Log) func() {
	return func() {
		total := config.setting.syncNum
		curNum := int64(0)

		syncedList := make([]string, 0)
		syncResourceMap.Range(func(key, value any) bool {
			reqPath := key.(string)
			log.Errorf("syncResource: %s", reqPath)
			err := config.targetClient.Get(reqPath, nil, func(statusCode int, responseHeaders http.Header, responseBody []byte) {
				if statusCode != 200 {
					return
				}

				info, ok := value.(map[string]interface{})
				if !ok {
					log.Errorf("syncResource value type error")
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
			if curNum >= total {
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

	log.Errorf("onHttpRequestHeaders check s3 path: %s", ctx.Path())
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
	status, err := proxywasm.GetHttpResponseHeader(":status")
	if err != nil {
		log.Errorf("onHttpResponseHeaders get status failed %s", err.Error())
		return types.ActionContinue
	}
	if status == "200" {
		ctx.SetContext("remote_file_exists", true)

		content, err := proxywasm.GetHttpResponseHeader("content-type")
		if err != nil {
			log.Errorf("onHttpResponseHeaders get content type failed %s", err.Error())
			return types.ActionContinue
		}

		ctx.SetContext("remote_file_content_type", content)
	}

	return types.ActionContinue
}

func onHttpResponseBody(ctx wrapper.HttpContext, config W7ProxyCache, body []byte, log wrapper.Log) types.Action {
	reqPath := ctx.GetStringContext("req_path", "")
	log.Errorf("onHttpResponseBody complete %s", reqPath)
	if reqPath == "" {
		return types.ActionContinue
	}

	data := ctx.GetContext("remote_file_exists")
	if data != nil {
		remoteFileExists, ok := data.(bool)
		if ok && !remoteFileExists {
			return types.ActionContinue
		}
	}
	data = ctx.GetContext("s3_file_exists")
	if data != nil {
		s3FileExists, ok := data.(bool)
		if ok && s3FileExists {
			return types.ActionContinue
		}
	}

	headers := make([][2]string, 0)
	contentType := ctx.GetStringContext("remote_file_content_type", "")
	if contentType != "" {
		headers = append(headers, [2]string{"Content-Type", contentType})
	}

	syncResourceMap.Store(reqPath, map[string]interface{}{
		"headers": headers,
	})

	return types.ActionContinue
}
