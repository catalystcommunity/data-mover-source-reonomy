package reonomydmsource

import (
	"encoding/base64"
	"fmt"
	"sync"
)

const reonomyBaseURL = "https://api.reonomy.com"
const reonomySearchSummariesPath = "/v2/search/summaries"
const reonomyPropertyBulkPath = "/v2/property/bulk"

// ReonomySource struct for use by the data-mover-core
type ReonomySource struct {
	AccessKey string
	SecretKey string
	BaseURL   string

	// summary query to filter properties
	SummaryQuery map[string]interface{}
	// limit of how many properties to retrieve per API call
	SummaryLimit int
	// property detail types. Valid types are basic, mortgages, ownership, reported_owner, sales, taxes, and tenants.
	PropertyDetailTypes []string
	// whether to supply the filter_pii to the property bulk API endpoint
	FilterPII bool

	// the access key and secret key combined and encoded in base64 for use in basic auth
	apiToken string
	// sync mutex to ensure that only one worker ever queries the summary api at a time
	// we only support a single thread communicating to the summary api due to limitations of reonomy's pagination
	summaryMu sync.Mutex
	// used by the GetData function to keep track of results from the summary query
	summarySearchToken *string
	// keeps track of when the summary search token returns empty
	summarySearchComplete bool
}

// NewReonomySource initializes a ReonomySource struct for use by the data-mover-core. The accessKey and secretKey are
// used for basic authentication to the API.
// The query parameter is used to filter which properties to return. The query is passed into the "settings" field in
// the body of a summaries search.
// Ref: https://api.reonomy.com/v2/docs/guides/search/#filtered-search.
// The detailTypes parameter must be one of basic, mortgages, ownership, reported_owner, sales, taxes, and tenants.
// Ref: https://api.reonomy.com/v2/docs/api/data-dictionary/
func NewReonomySource(accessKey string, secretKey string, query map[string]interface{}, propertyDetailTypes []string, filterPII bool, summaryLimit int) *ReonomySource {
	return &ReonomySource{
		AccessKey:           accessKey,
		SecretKey:           secretKey,
		BaseURL:             reonomyBaseURL,
		SummaryQuery:        query,
		PropertyDetailTypes: propertyDetailTypes,
		FilterPII:           filterPII,
		SummaryLimit:        summaryLimit,
	}
}

// Initialize is the implementation of the Initialize interface for the data-mover-core
func (s *ReonomySource) Initialize() error {
	s.apiToken = base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", s.AccessKey, s.SecretKey)))
	s.summarySearchComplete = false
	return nil
}

// GetData is the implementation of the GetData interface for the data-mover-core
func (s *ReonomySource) GetData() (data []map[string]interface{}, err error) {
	// return empty data when we've exhausted the last of the search tokens
	if s.summarySearchComplete {
		return
	}

	propertyIDs, err := s.getSummaryIDs()
	if err != nil {
		return
	}

	data, err = s.getPropertyBulk(propertyIDs)
	return
}
