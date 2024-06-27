package handler

type NOP struct{}

func (n *NOP) Decode(report map[string]interface{}) (interface{}, error) {
	return nil, nil
}

func (n *NOP) Aggregate(aggReport interface{}, report interface{}) error {
	return nil
}
