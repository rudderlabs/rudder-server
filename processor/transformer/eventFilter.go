package transformer

func (event TransformerEventT) serverSideIdentify() bool {
	serverSideIdentify, flag := event.Destination.Config["enableServerSideIdentify"]
	if flag {
		flagVal, ok := serverSideIdentify.(bool)
		if !ok {
			return true
		} else {
			return flagVal
		}
	}
	return true
}

func (event TransformerEventT) DestinationEventTypeRulesFilter(eventType string) bool {
	switch eventType {
	case "identify":
		return event.serverSideIdentify()
	}
	return true
}
