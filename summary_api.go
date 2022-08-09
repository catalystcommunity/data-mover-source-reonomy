package reonomydmsource

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/catalystsquad/app-utils-go/logging"
	"github.com/joomcode/errorx"
)

type summaryBody struct {
	Limit    int                    `json:"limit"`
	Settings map[string]interface{} `json:"settings"`
}

type summaryResponse struct {
	// the summary response has many return fields that we don't currently use,
	// so we only specify the ones we need to unmarshal
	SearchToken string `json:"search_token"`
	Count       int    `json:"count"`
	Items       []struct {
		ID string `json:"id"`
	} `json:"items"`
}

func (s *ReonomySource) getSummaryIDs(query map[string]interface{}) (ids []string, err error) {
	// lock the mutex to ensure only one thread ever queries the summary endpoint
	s.summaryMu.Lock()
	defer s.summaryMu.Unlock()

	summaryBody := summaryBody{
		Limit:    s.SummaryLimit,
		Settings: query,
	}

	reqBody, err := json.Marshal(summaryBody)
	if err != nil {
		return
	}

	logging.Log.Debug("sending summary api request")
	body, statusCode, requestErr := s.doPostRequestHandleRateLimit(s.getSummarySearchTokenURL(), reqBody, 1, 5)
	if requestErr != nil {
		err = requestErr
		return
	}
	if statusCode != http.StatusOK {
		err = errorx.Decorate(err, "unexpected status code from summary api request: %d with body: %s", statusCode, body)
		return
	}

	var response summaryResponse
	err = json.Unmarshal(body, &response)
	if err != nil {
		return
	}

	logging.Log.Debugf("got response: %#v", response)

	// save the search token for future executions
	s.summarySearchToken = response.SearchToken

	for _, i := range response.Items {
		ids = append(ids, i.ID)
	}
	return
}

func (s *ReonomySource) getSummaryURL() string {
	return fmt.Sprintf("%s%s", s.BaseURL, reonomySearchSummariesPath)
}

func (s *ReonomySource) getSummarySearchTokenURL() string {
	if s.summarySearchToken != "" {
		// url encode the query
		params := url.Values{}
		params.Add("search_token", s.summarySearchToken)
		return fmt.Sprintf("%s?%s", s.getSummaryURL(), params.Encode())
	}
	return s.getSummaryURL()
}
