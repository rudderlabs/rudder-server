package router

type RouterAdmin struct {
	handle *HandleT
}

// Status function is used for debug purposes by the admin interface
func (r *RouterAdmin) Status() map[string]interface{} {
	return map[string]interface{}{}

}
