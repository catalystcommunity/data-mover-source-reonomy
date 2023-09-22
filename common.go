package reonomydmsource

import (
	"fmt"
	"net/http"
	"time"

	"github.com/catalystsquad/app-utils-go/logging"
	"github.com/valyala/fasthttp"
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
		if err == nil && statusCode == http.StatusOK {
			return respBody, statusCode, nil
		}

		logging.Log.WithError(err).Errorf("attempt %d received HTTP %d response", i, statusCode)
		if i < maxAttempts-1 { // don't sleep if this is the last attempt
			logging.Log.Infof("backing off for %d seconds", backoffDurationSeconds)
			logging.Log.Debugf("error response body: %s", string(respBody))
			time.Sleep(time.Second * time.Duration(backoffDurationSeconds))
			logging.Log.Info("retrying request")
		}
	}
	err = fmt.Errorf("max attempts reached, aborting request")
	return nil, statusCode, err
}
