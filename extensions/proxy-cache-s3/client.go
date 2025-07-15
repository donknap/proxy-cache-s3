package main

import (
	"fmt"
	"github.com/alibaba/higress/plugins/wasm-go/pkg/wrapper"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

var originClientMap = sync.Map{}

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

func responseS3Resource(s3StatusCode int, s3ResponseHeaders http.Header, s3ResponseBody []byte, log wrapper.Log) {
	headers := make([][2]string, 0)
	for key, item := range s3ResponseHeaders {
		headers = append(headers, [2]string{key, item[0]})
	}
	err := proxywasm.SendHttpResponse(uint32(s3StatusCode), headers, s3ResponseBody, -1)
	if err != nil {
		log.Errorf("onHttpRequestHeaders send response failed %s", err.Error())
	}
}
