package util

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

var reservedObjectNames = regexp.MustCompile("^[a-zA-Z0-9-_.~/]+$")

// GeneratePresignedURL 生成一个 AWS S3 预签名 URL
func GeneratePresignedURL(accessKey, secretKey, sessionToken, region, host, bucket, key string, method string, expires time.Duration, versionID string) (string, error) {
	signV4Algorithm := "AWS4-HMAC-SHA256"
	iso8601DateFormat := "20060102T150405Z"
	yyyymmdd := "20060102"
	ServiceTypeS3 := "s3"

	// 生成当前时间
	t := time.Now().UTC()
	credentialDate := t.Format("20060102")

	endpoint := fmt.Sprintf("http://%s/%s%s", host, bucket, key)
	parsedURL, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}

	// 构建 Canonical Request
	credential := fmt.Sprintf("%s/%s/%s/%s/aws4_request", accessKey, credentialDate, region, ServiceTypeS3)
	signedHeaders := "host"
	query := parsedURL.Query()
	query.Set("X-Amz-Algorithm", signV4Algorithm)
	query.Set("X-Amz-Date", t.Format(iso8601DateFormat))
	query.Set("X-Amz-Expires", strconv.FormatInt(int64(expires/time.Second), 10))
	query.Set("X-Amz-SignedHeaders", signedHeaders)
	query.Set("X-Amz-Credential", credential)
	if sessionToken != "" {
		query.Set("X-Amz-Security-Token", sessionToken)
	}
	// 如果有 versionID，添加到 URL
	if versionID != "" {
		query.Set("versionId", url.QueryEscape(versionID))
	}
	rawQuery := query.Encode()
	rawQuery = strings.ReplaceAll(rawQuery, "+", "%20")

	canonicalHeaders := fmt.Sprintf("host:%s\n", host)
	payloadHash := "UNSIGNED-PAYLOAD"
	canonicalRequest := strings.Join([]string{
		strings.ToUpper(method),
		encodePath(parsedURL.Path),
		rawQuery,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")

	scope := strings.Join([]string{
		t.Format(yyyymmdd),
		region,
		ServiceTypeS3,
		"aws4_request",
	}, "/")
	stringToSign := signV4Algorithm + "\n" + t.Format(iso8601DateFormat) + "\n"
	stringToSign = stringToSign + scope + "\n"
	stringToSign += hashSHA256(canonicalRequest)

	signingKey := getSignatureKey(secretKey, credentialDate, region, ServiceTypeS3)
	signature := hmacSHA256(signingKey, stringToSign)
	signatureHex := hex.EncodeToString(signature)

	// 构建预签名 URL
	parsedURL.RawQuery = rawQuery + "&X-Amz-Signature=" + signatureHex

	return parsedURL.String(), nil
}

func encodePath(pathName string) string {
	if reservedObjectNames.MatchString(pathName) {
		return pathName
	}
	var encodedPathname strings.Builder
	for _, s := range pathName {
		if 'A' <= s && s <= 'Z' || 'a' <= s && s <= 'z' || '0' <= s && s <= '9' { // §2.3 Unreserved characters (mark)
			encodedPathname.WriteRune(s)
			continue
		}
		switch s {
		case '-', '_', '.', '~', '/': // §2.3 Unreserved characters (mark)
			encodedPathname.WriteRune(s)
			continue
		default:
			l := utf8.RuneLen(s)
			if l < 0 {
				// if utf8 cannot convert return the same string as is
				return pathName
			}
			u := make([]byte, l)
			utf8.EncodeRune(u, s)
			for _, r := range u {
				hex := hex.EncodeToString([]byte{r})
				encodedPathname.WriteString("%" + strings.ToUpper(hex))
			}
		}
	}
	return encodedPathname.String()
}

// hashSHA256 对输入字符串进行 SHA256 哈希
func hashSHA256(data string) string {
	hash := sha256.New()
	hash.Write([]byte(data))
	return hex.EncodeToString(hash.Sum(nil))
}

// hmacSHA256 对输入字符串进行 HMAC-SHA256 加密
func hmacSHA256(key []byte, message string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(message))
	return h.Sum(nil)
}

// getSignatureKey 生成签名密钥
func getSignatureKey(secretKey, date, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secretKey), date)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	kSigning := hmacSHA256(kService, "aws4_request")
	return kSigning
}
