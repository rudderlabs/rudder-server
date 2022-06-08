package gateway

//easyjson:json
type batch struct {
	Entries []batchEntry `json:"batch"`
}

//easyjson:json
type batchEntry struct {
	UserID      string `json:"userId"`
	AnonymousID string `json:"anonymousId"`
}
