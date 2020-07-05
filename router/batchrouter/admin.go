package batchrouter

type BatchRouterAdmin struct {
	handle *HandleT
}

// Status function is used for debug purposes by the admin interface
func (b *BatchRouterAdmin) Status() map[string]interface{} {
	return map[string]interface{}{}

}
