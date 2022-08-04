package reonomydmsource

import (
	"encoding/json"
	"fmt"
	"github.com/joomcode/errorx"
	"net/http"
)

type propertyBulkBody struct {
	PropertyIds []string `json:"property_ids"`
	DetailType  []string `json:"detail_type"`
	FilterPII   bool     `json:"filter_pii"`
}

func (s *ReonomySource) getPropertyBulk(IDs []string) (properties []map[string]interface{}, err error) {
	query := propertyBulkBody{
		PropertyIds: IDs,
		DetailType:  s.PropertyDetailTypes,
		FilterPII:   s.FilterPII,
	}

	reqBody, err := json.Marshal(query)
	if err != nil {
		return
	}

	req := createPostRequest(s.getPropertyBulkPath(), reqBody)

	body, statusCode, requestErr := s.sendRequestHandleRateLimit(req)
	if requestErr != nil {
		err = requestErr
		return
	}
	if statusCode != http.StatusOK {
		err = errorx.Decorate(err, "unexpected status code: %d with body: %s", statusCode, body)
		return
	}

	err = json.Unmarshal(body, &properties)
	return
}

func (s *ReonomySource) getPropertyBulkPath() string {
	return fmt.Sprintf("%s%s", s.BaseURL, reonomyPropertyBulkPath)
}
