package reporting

import (
	"fmt"
	"strings"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/tidwall/gjson"
)

const (
	objectKeyWildcard   = "*"
	arrayWildcard       = "#"
	destinationResponse = "destinationResponse"
)

var (
	WildcardKeys            []string = []string{}
	defaultErrorMessageKeys          = []string{"message", "description", "detail", "title", "Error", "error", "error_message"}
)

type ExtractorT struct {
	WildcardKeys     []string
	MaxLevel         int      // Max level of wild-card search
	ErrorMessageKeys []string // the keys where in we may have error message
}

func NewErrorDetailExtractor() *ExtractorT {
	errMsgKeys := config.GetStringSlice("Reporting.ErrorDetail.ErrorMessageKeys", []string{})
	maxLevel := config.GetInt("Reporting.ErrorDetail.MaxLevel", 2)
	defaultErrorMessageKeys = append(defaultErrorMessageKeys, errMsgKeys...)
	extractor := &ExtractorT{
		ErrorMessageKeys: defaultErrorMessageKeys,
		MaxLevel:         maxLevel,
	}
	extractor.BuildMessageKeysPermutations()
	return extractor
}

func generateCombinations(symbols, res []string, prefix string, level int) []string {
	if level <= 0 {
		// fmt.Printf("Prefix:%s\n", prefix)
		res = append(res, prefix)
		return res
	}
	// var combinedStr string
	for _, sym := range symbols {
		newPrefix := fmt.Sprintf("%s%s", prefix, sym)
		// fmt.Printf("NewPrefix:%s\n", newPrefix)
		res = generateCombinations(symbols, res, newPrefix, level-1)
	}

	// return strings.Join(strings.Split(combinedStr, ""), ".")
	return res
}

// This function is to be executed only once
// as these keys will be same across destinations
func (t *ExtractorT) BuildMessageKeysPermutations() {
	var level int
	// sts := []string{"", objectKeyWildcard, arrayWildcard}

	results := []string{}
	for level <= t.MaxLevel {
		results = append(results, generateCombinations([]string{objectKeyWildcard, arrayWildcard}, []string{}, "", level)...)
		// sts = append(sts, combinations)
		level++
	}

	correctedCombinations := []string{}
	for _, comb := range results {
		correctedCombinations = append(correctedCombinations, strings.Join(strings.Split(comb, ""), "."))
	}

	wildcardKeys := []string{}

	for _, probableKey := range t.ErrorMessageKeys {
		for _, comb := range correctedCombinations {
			if len(comb) == 0 {
				wildcardKeys = append(wildcardKeys, probableKey)
				continue
			}
			wildcardKeys = append(wildcardKeys, fmt.Sprintf("%s.%s", comb, probableKey))
		}
	}

	// We want to check the same combinations in transformer proxy destination's response
	// proxy return `{message: "", destinationResponse: "{}", ...}`
	// destinationResponse in returned response, contains the raw destination response
	// Preference given destinationResponse.* than any other probable error message keys
	destRespIncludedWildcardKeys := []string{}
	for _, wildCardKey := range wildcardKeys {
		destRespWildcardKey := fmt.Sprintf("%s.%s", destinationResponse, wildCardKey)
		destRespIncludedWildcardKeys = append(destRespIncludedWildcardKeys, destRespWildcardKey)
	}
	destRespIncludedWildcardKeys = append(destRespIncludedWildcardKeys, wildcardKeys...)

	t.WildcardKeys = destRespIncludedWildcardKeys
}

func (t *ExtractorT) GetErrorMessageFromResponse(response string) string {
	if len(t.WildcardKeys) == 0 {
		panic(fmt.Errorf("BuildMessageKeysPermutations method has to be executed before we can execute any methods"))
	}
	// For now we will be getting error message

	// invalid json string case
	if !gjson.Valid(response) {
		return response
	}
	probableKeyList := []string{"response", "response.error", "response.message"}
	probableKeyList = append(probableKeyList, t.WildcardKeys...)

	prelimResults := gjson.GetMany(response, probableKeyList...)
	for _, prelimResult := range prelimResults {
		if prelimResult.Exists() {
			switch prelimResult.Type {
			case gjson.String:
				return prelimResult.String()
			case gjson.JSON:
				if prelimResult.IsArray() {
					// returning the first result
					arr := prelimResult.Array()
					if len(arr) == 0 {
						continue
					}
					// if there are multiple error strings inside
					return t.GetErrorMessageFromResponse(prelimResult.Array()[0].String())
				}
				if prelimResult.IsObject() {
					return t.GetErrorMessageFromResponse(prelimResult.String())
				}
			}
		}
	}

	return ""
}
