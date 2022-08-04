package reonomydmsource

import (
	"encoding/json"
	"fmt"
	"github.com/joomcode/errorx"
	"net/http"
	"net/url"
)

type summaryBody struct {
	Limit    int                    `json:"limit"`
	Settings map[string]interface{} `json:"settings"`
}

type summaryResponse struct {
	// the summary response has many return fields that we don't currently use, so we only specify the ones we need to unmarshal
	SearchToken string `json:"search_token"`
	Items       []struct {
		ID string `json:"id"`
	} `json:"items"`
}

func (s *ReonomySource) getSummaryIDs() (IDs []string, err error) {
	// lock the mutex to ensure only one thread ever queries the summary endpoint
	s.summaryMu.Lock()
	defer s.summaryMu.Unlock()

	query := summaryBody{
		Limit:    s.SummaryLimit,
		Settings: s.SummaryQuery,
	}

	reqBody, err := json.Marshal(query)
	if err != nil {
		return
	}

	req := createPostRequest(s.getSummarySearchTokenURL(), reqBody)

	body, statusCode, requestErr := s.sendRequestHandleRateLimit(req)
	if requestErr != nil {
		err = requestErr
		return
	}
	if statusCode != http.StatusOK {
		err = errorx.Decorate(err, "unexpected status code: %d with body: %s", statusCode, body)
		return
	}

	var response summaryResponse
	err = json.Unmarshal(body, &response)
	if err != nil {
		return
	}

	// save the search token for future executions
	*s.summarySearchToken = response.SearchToken
	// set summarySearchComplete to true when the search token is empty, because there should be no more results to query
	if response.SearchToken == "" {
		s.summarySearchComplete = true
	}

	for _, i := range response.Items {
		IDs = append(IDs, i.ID)
	}
	return
}

func (s *ReonomySource) getSummaryURL() string {
	return fmt.Sprintf("%s%s", s.BaseURL, reonomySearchSummariesPath)
}

func (s *ReonomySource) getSummarySearchTokenURL() string {
	if s.summarySearchToken != nil {
		// url encode the query
		params := url.Values{}
		params.Add("search_token", *s.summarySearchToken)
		return fmt.Sprintf("%s?%s", s.getSummaryURL(), params.Encode())
	}
	return s.getSummaryURL()
}
