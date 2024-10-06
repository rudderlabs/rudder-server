package marketobulkupload

type MarketoConfig struct {
	ClientId           string            `json:"clientId"`
	ClientSecret       string            `json:"clientSecret"`
	MunchkinId         string            `json:"munchkinId"`
	DeduplicationField string            `json:"deDuplicationField"`
	FieldsMapping      map[string]string `json:"columnFieldsMapping"`
}

type intermediateMarketoConfig struct {
	ClientId            string              `json:"clientId"`
	ClientSecret        string              `json:"clientSecret"`
	MunchkinId          string              `json:"munchkinId"`
	DeduplicationField  string              `json:"deDuplicationField"`
	ColumnFieldsMapping []map[string]string `json:"columnFieldsMapping"`
}

type MarketoResponse struct {
	Errors        []Error   `json:"errors"`
	MoreResult    bool      `json:"moreResult"`
	NextPageToken string    `json:"nextPageToken"`
	RequestID     string    `json:"requestId"`
	Result        []Result  `json:"result"`
	Success       bool      `json:"success"`
	Warnings      []Warning `json:"warnings"`
}

type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type Result struct {
	BatchID              int    `json:"batchId"`
	ImportID             string `json:"importId"`
	Message              string `json:"message"`
	NumOfLeadsProcessed  int    `json:"numOfLeadsProcessed"`
	NumOfRowsFailed      int    `json:"numOfRowsFailed"`
	NumOfRowsWithWarning int    `json:"numOfRowsWithWarning"`
	Status               string `json:"status"`
}

type Warning struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}
