package reonomydmsource

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/catalystcommunity/app-utils-go/logging"
	"github.com/joomcode/errorx"
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

	logging.Log.Debug("sending bulk api request")
	body, statusCode, requestErr := s.doPostRequestHandleRateLimit(s.getPropertyBulkPath(), reqBody, 30, 3)
	if requestErr != nil {
		err = requestErr
		return
	}
	if statusCode != http.StatusOK {
		err = errorx.Decorate(err, "unexpected status code from bulk api request: %d with body: %s", statusCode, body)
		return
	}

	err = json.Unmarshal(body, &properties)
	logging.Log.Debugf("got %d properties", len(properties))
	return
}

func (s *ReonomySource) getPropertyBulkPath() string {
	return fmt.Sprintf("%s%s", s.BaseURL, reonomyPropertyBulkPath)
}
