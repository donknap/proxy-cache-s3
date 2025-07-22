package main

import (
	"fmt"
	"github.com/alibaba/higress/plugins/wasm-go/pkg/wrapper"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

var originClientMap = sync.Map{}

func SliceToHeader(slice [][2]string) http.Header {
	header := make(http.Header)
	for _, pair := range slice {
		key := pair[0]
		value := pair[1]
		header.Add(key, value)
	}
	return header
}

func HeaderToSlice(header http.Header) [][2]string {
	slice := make([][2]string, 0, len(header))
	for key, values := range header {
		for _, value := range values {
			slice = append(slice, [2]string{key, value})
		}
	}
	return slice
}

func GetOriginalRequestHeaders() http.Header {
	originalHeaders, _ := proxywasm.GetHttpRequestHeaders()
	return SliceToHeader(originalHeaders)
}

func OverwriteRequestHostHeader(headers http.Header, host string) {
	//if originHost, err := proxywasm.GetHttpRequestHeader(":authority"); err == nil {
	//	headers.Set("X-ENVOY-ORIGINAL-HOST", originHost)
	//}
	headers.Set(":authority", host)
}

func OverwriteRequestPathHeader(headers http.Header, path string) {
	//if originPath, err := proxywasm.GetHttpRequestHeader(":path"); err == nil {
	//	headers.Set("X-ENVOY-ORIGINAL-PATH", originPath)
	//}
	headers.Set(":path", path)
}

func ReplaceRequestHeaders(headers http.Header) {
	modifiedHeaders := HeaderToSlice(headers)
	_ = proxywasm.ReplaceHttpRequestHeaders(modifiedHeaders)
}

func getOriginClient(clusterName string, originHost string) (wrapper.HttpClient, error) {
	_originClient, exists := originClientMap.Load(clusterName)
	if !exists {
		clusterInfo := strings.Split(clusterName, "|")
		if len(clusterInfo) != 4 {
			return nil, fmt.Errorf("cluster %s is not valid", clusterName)
		}
		port, err := strconv.Atoi(clusterInfo[1])
		if err != nil {
			return nil, fmt.Errorf("invalid port: %s", clusterInfo[1])
		}

		serviceName := strings.ReplaceAll(clusterInfo[3], ".dns", "")
		_originClient = wrapper.NewClusterClient(wrapper.DnsCluster{
			Port:        int64(port),
			ServiceName: serviceName,
			Domain:      originHost,
		})
		originClientMap.Store(clusterName, _originClient)
	}

	return _originClient.(wrapper.HttpClient), nil
}

func responseS3Resource(config W7ProxyCache, s3SavePath string) error {
	tmpConfig := config
	tmpConfig.setting.host = config.setting.originHost
	getS3PresignPath, err := getS3PresignedURL(tmpConfig, s3SavePath, "GET", 360*time.Second)
	u, err := url.Parse(getS3PresignPath)
	if err != nil {
		return err
	}

	reqHeaders := GetOriginalRequestHeaders()
	OverwriteRequestPathHeader(reqHeaders, u.RequestURI())
	reqHeaders.Set("User-Agent", "test")
	ReplaceRequestHeaders(reqHeaders)

	wrapper.Log{}.Errorf("onHttpRequestHeaders s3 file exists response: %s", s3SavePath)

	return nil
}
