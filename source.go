package reonomydmsource

import (
	"encoding/base64"
	"fmt"
	"sync"

	"github.com/catalystsquad/app-utils-go/logging"
)

const reonomyBaseURL = "https://api.reonomy.com"
const reonomySearchSummariesPath = "/v2/search/summaries"
const reonomyPropertyBulkPath = "/v2/property/bulk"

// ReonomySource struct for use by the data-mover-core
type ReonomySource struct {
	AccessKey string
	SecretKey string
	BaseURL   string

	// summary queries to filter properties
	SummaryQueries []map[string]interface{}
	// limit of how many properties to retrieve per API call
	SummaryLimit int
	// property detail types. Valid types are basic, mortgages, ownership,
	// reported_owner, sales, taxes, and tenants.
	PropertyDetailTypes []string
	// whether to supply the filter_pii to the property bulk API endpoint
	FilterPII bool
	// the number of times to retry a request if it fails
	RetryCount int

	// the access key and secret key combined and encoded in base64 for use in
	// basic auth
	apiToken string
	// query index keeps track of which element of the queries we are on
	queryIndex int
	// sync mutex to ensure that only one worker ever queries the summary api
	// at a time we only support a single thread communicating to the summary
	// api due to limitations of reonomy's pagination
	summaryMu sync.Mutex
	// used by the GetData function to keep track of results from the summary
	// query
	summarySearchToken string
}

// NewReonomySource initializes a ReonomySource struct for use by the
// data-mover-core. The accessKey and secretKey are used for basic
// authentication to the API.
//
// The query parameter is used to filter which properties to return. The query
// is passed into the "settings" field in the body of a summaries search.
// Ref: https://api.reonomy.com/v2/docs/guides/search/#filtered-search.
//
// The detailTypes parameter must be one of:
// basic, mortgages, ownership, reported_owner, sales, taxes, and tenants.
// Ref: https://api.reonomy.com/v2/docs/api/data-dictionary/
func NewReonomySource(accessKey string, secretKey string, queries []map[string]interface{}, propertyDetailTypes []string, filterPII bool, summaryLimit int) *ReonomySource {
	return &ReonomySource{
		AccessKey:           accessKey,
		SecretKey:           secretKey,
		BaseURL:             reonomyBaseURL,
		SummaryQueries:      queries,
		PropertyDetailTypes: propertyDetailTypes,
		FilterPII:           filterPII,
		SummaryLimit:        summaryLimit,
	}
}

// Initialize is the implementation of the Initialize interface for the data-mover-core
func (s *ReonomySource) Initialize() error {
	s.apiToken = base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", s.AccessKey, s.SecretKey)))
	s.queryIndex = 0
	return nil
}

// GetData is the implementation of the GetData interface for the data-mover-core
func (s *ReonomySource) GetData() (data []map[string]interface{}, err error) {
	if s.queryIndex >= len(s.SummaryQueries) {
		// return empty data when we've exhausted all queries
		return data, nil
	}

	var propertyIDs []string
	// loop until we get a summary query that returns with property IDs
	for {
		// get propertyIDs from current query
		for i := 0; i <= s.RetryCount; i++ {
			propertyIDs, err = s.getSummaryIDs(s.SummaryQueries[s.queryIndex])
			if err != nil {
				logging.Log.WithError(err).Error("error getting summary IDs")
				continue // retry
			}
		}
		// if we made it out of the loop with an error, then we've exhausted
		// the retries and should fail.
		if err != nil {
			return nil, err
		}

		if len(propertyIDs) > 0 {
			// iterate the query index if there was no search token in the last
			// summary response
			if s.summarySearchToken == "" {
				s.queryIndex++
			}
			// break the loop when we get property IDs returned
			break
		}

		// iterate the queryIndex if we don't get property IDs
		s.queryIndex++
		// reset search token, incase reonomy gave us one even though it didn't
		// return with any data
		s.summarySearchToken = ""
		if s.queryIndex >= len(s.SummaryQueries) {
			// return empty data when we've exhausted all queries
			return data, nil
		}
	}

	for i := 0; i <= s.RetryCount; i++ {
		data, err = s.getPropertyBulk(propertyIDs)
		if err != nil {
			logging.Log.WithError(err).Error("error getting bulk properties")
			continue // retry
		}
		return data, nil
	}
	return nil, err
}
