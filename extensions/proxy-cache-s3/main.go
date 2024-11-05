package main

import (
	"github.com/alibaba/higress/plugins/wasm-go/pkg/wrapper"
	"github.com/donknap/proxy-cache-s3/util"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm/types"
	"github.com/tidwall/gjson"
	"net/http"
	"strings"
	"time"
)

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
	client   wrapper.HttpClient
	cacheKey []string
	setting  struct {
		cacheTTL       int64
		cacheHeader    bool
		accessKey      string
		secretKey      string
		region         string
		bucket         string
		host           string
		port           int64
		purgeReqMethod string
	}
}

func parseConfig(json gjson.Result, config *W7ProxyCache, log wrapper.Log) error {
	urlServiceInfo := strings.Replace("", ".svc.cluster.local", config.setting.host, 1)
	urlServiceInfoArr := strings.Split(urlServiceInfo, ".")
	if len(urlServiceInfoArr) != 2 {
		log.Errorf("invalid host: %s", config.setting.host)
		return types.ErrorStatusBadArgument
	}
	config.client = wrapper.NewClusterClient(wrapper.K8sCluster{
		Port:        80,
		Version:     "",
		ServiceName: urlServiceInfoArr[0],
		Namespace:   urlServiceInfoArr[1],
	})

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

	if config.setting.accessKey == "" ||
		config.setting.secretKey == "" ||
		config.setting.region == "" ||
		config.setting.bucket == "" ||
		config.setting.host == "" {
		log.Error("s3 setting is empty")
		return types.ErrorStatusBadArgument
	}

	if config.setting.port == 0 {
		config.setting.port = 80
	}

	return nil
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

	log.Infof("onHttpRequestHeaders check s3 path: %s", ctx.Path())
	log.Infof("onHttpRequestHeaders check s3 remote path : %s", checkExistsUrl)
	err = config.client.Get(checkExistsUrl, nil, func(statusCode int, responseHeaders http.Header, responseBody []byte) {
		exists := false
		if statusCode == 200 {
			exists = true
		}
		modifiedAt := responseHeaders.Get("last-modified")
		if modifiedAt == "" {
			exists = false
		}
		log.Infof("onHttpRequestHeaders check s3 complete: %s, %d, %s", ctx.Path(), statusCode, modifiedAt)

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
			log.Infof("onHttpRequestHeaders s3 file exists: %s", ctx.Path())

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

	return types.HeaderStopAllIterationAndBuffer
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

	putPath, err := util.GeneratePresignedURL(
		config.setting.accessKey,
		config.setting.secretKey,
		"",
		config.setting.region,
		config.setting.host,
		config.setting.bucket,
		ctx.Path(),
		"PUT",
		3600*time.Second,
		"",
	)
	if err != nil {
		log.Errorf("onHttpResponseBody make s3 url failed: %v", err)
		return types.ActionContinue
	}

	headers := make([][2]string, 0)
	contentType := ctx.GetStringContext("remote_file_content_type", "")
	if contentType != "" {
		headers = append(headers, [2]string{"Content-Type", contentType})
	}
	err = config.client.Put(putPath, headers, body, func(statusCode int, responseHeaders http.Header, responseBody []byte) {
		log.Infof("onHttpResponseBody sync complete: %d, %s", statusCode, ctx.Path())

		err := proxywasm.ResumeHttpResponse()
		if err != nil {
			log.Errorf("onHttpResponseBody resume response failed %s", err.Error())
			return
		}
	})
	if err != nil {
		return types.ActionContinue
	}

	return types.DataStopIterationAndBuffer
}
