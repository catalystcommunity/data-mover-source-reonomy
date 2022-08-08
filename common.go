package reonomydmsource

import (
	"fmt"
	"github.com/catalystsquad/app-utils-go/logging"
	"github.com/valyala/fasthttp"
	"net/http"
	"time"
)

func (s *ReonomySource) doPostRequest(uri string, reqBody []byte) (respBody []byte, statusCode int, err error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.SetRequestURI(uri)
	req.Header.SetMethod(http.MethodPost)
	req.Header.SetContentType("application/json")
	req.SetBody(reqBody)
	req.Header.Add("Authorization", fmt.Sprintf("Basic %s", s.apiToken))
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(res)
	err = fasthttp.Do(req, res)
	return res.Body(), res.StatusCode(), err
}

func (s *ReonomySource) doPostRequestHandleRateLimit(uri string, reqBody []byte, backoffDurationSeconds int, maxAttempts int) (respBody []byte, statusCode int, err error) {
	for i := 0; i < maxAttempts; i++ {
		respBody, statusCode, err = s.doPostRequest(uri, reqBody)
		if statusCode != http.StatusTooManyRequests {
			return
		}
		logging.Log.Info(fmt.Sprintf("received HTTP 429 response, backing off for %d second...", backoffDurationSeconds))
		logging.Log.Debug(fmt.Sprintf("429 response body: %s", string(respBody)))
		time.Sleep(time.Second * time.Duration(backoffDurationSeconds))
		logging.Log.Info("retrying request...")
	}
	return
}
