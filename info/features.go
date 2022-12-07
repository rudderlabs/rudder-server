package info

type Component struct {
	Name     string
	Features []string
}

// ServerComponent describes server component info, like implemented features.
var ServerComponent = Component{
	Name: "server",
	Features: []string{
		"gzip-req-payload",
	},
}

// WarehouseComponent describes server component info, like implemented features.
var WarehouseComponent = Component{
	Name: "warehouse",
	Features: []string{
		"retryUI",
		"configurationTests",
		"azureSASTokens",
	},
}
