package warehouse

type WarehouseAdmin struct{}

func (wh *WarehouseAdmin) TriggerUpload(arg struct{}, reply *string) error {
	startUploadAlways = true
	*reply = "Successfully set uploads to start always without delay"
	return nil
}
