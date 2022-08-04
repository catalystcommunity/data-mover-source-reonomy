package reonomydmsource

import (
	"fmt"
	"github.com/catalystsquad/app-utils-go/logging"
	"github.com/valyala/fasthttp"
	"net/http"
	"time"
)

func createPostRequest(uri string, body []byte) *fasthttp.Request {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.SetRequestURI(uri)
	req.Header.SetMethod(http.MethodPost)
	req.Header.SetContentType("application/json")
	req.SetBody(body)
	return req
}

func (s *ReonomySource) sendRequest(req *fasthttp.Request) ([]byte, int, error) {
	req.Header.Add("Authorization", fmt.Sprintf("Basic %s", s.apiToken))
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(res)
	err := fasthttp.Do(req, res)
	return res.Body(), res.StatusCode(), err
}

func (s *ReonomySource) sendRequestHandleRateLimit(req *fasthttp.Request) (body []byte, statusCode int, err error) {
	for i := 0; i < 5; i++ {
		body, statusCode, err = s.sendRequest(req)
		if statusCode != http.StatusTooManyRequests {
			break
		}
		logging.Log.Info("received HTTP 429 response, backing off for 1 second...")
		time.Sleep(time.Second)
	}
	return
}
