package warehouseutils_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/rudderlabs/rudder-go-kit/awsutil"
	"github.com/rudderlabs/rudder-go-kit/logger"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-server/utils/misc"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	. "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestSanitizeJSON(t *testing.T) {
	testCases := []struct {
		name     string
		input    json.RawMessage
		expected json.RawMessage
	}{
		{
			name:     "empty json",
			input:    json.RawMessage(`{}`),
			expected: json.RawMessage(`{}`),
		},
		{
			name:     "with unicode characters",
			input:    json.RawMessage(`{"exporting_data_failed":{"attempt":1,"errors":["Start: \u0000\u0000\u0000\u0000\u0000\u0000\u0000 : End"]}}`),
			expected: json.RawMessage(`{"exporting_data_failed":{"attempt":1,"errors":["Start:  : End"]}}`),
		},
		{
			name:     "without unicode characters",
			input:    json.RawMessage(`{"exporting_data_failed":{"attempt":1,"errors":["Start:  : End"]}}`),
			expected: json.RawMessage(`{"exporting_data_failed":{"attempt":1,"errors":["Start:  : End"]}}`),
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tc.expected, SanitizeJSON(tc.input))
		})
	}
}

func TestSanitizeString(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:  "empty string",
			input: "",
		},
		{
			name:     "with unicode characters",
			input:    "Start: \u0000\u0000\u0000\u0000\u0000\u0000\u0000 : End",
			expected: "Start:  : End",
		},
		{
			name:     "without unicode characters",
			input:    "Start:  : End",
			expected: "Start:  : End",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tc.expected, SanitizeString(tc.input))
		})
	}
}

func TestFormatPemContent(t *testing.T) {
	testCases := []struct {
		name   string
		input  string
		output string
	}{
		{
			name:   "certificate (textInput)",
			input:  `-----BEGIN CERTIFICATE----- MIIFJDBWBgkqhkiG9w0BBQ0wSTAxBgkqhkiG9w0BBQwwJAQQ64IduEZCqZfR5Nix HyETFQICCAAwDAYIKoZIhvcNAgkFADAUBggqhkiG9w0DBwQILyNp24M/i6MEggTI xnr7+5ltJidFtjYBhUZJz0LxmQD+2PmH4D5a22lkewL22iLcdLuQbWdvGzXSaCmP IkhQ9k/KRrzda98IkUuZ3X3uqgvqeJjXh7nqCFahfTvZhfFaINkCqzowl03wB9Dn rKxIT9C7Z00m2CSdxL+cHSYAWl1GIn/d7j99bHWa/m7H5f3BuE/xStYJG1gHrC+g aB+dzZLDygiTYqc4SaTtHDBT1PeIJqiDQOgfKGF9vZKdtrfAjPlS2MI+s2AJHO3B h6zTMjhIrQ6RX/Y5gGm5DkVeuygP2unf8EBoOc5MOkeTb9hMqnKlBz2fumo/I5mu qo5DHk4EZpnqNymRaY0glp6bjd3vMj889kiZ2Rdtf4doieYuggwYBJ4WxpQ2GQAy RfGMcDOWHIwAe9p4+prgNet6Atvtp2YEH7LjXVvmNFTGbK6Jo+Fxp5n5B/3b54kB 6RFJa6JJz4YHHEU6w8woTanr/9W2urKncbQXQW5kUW2YBSCz3ZWHNr93AY9IVXa2 csAuAcQng/8coR2A20QJybntYLHUl1p0bZMM2XgaCeqD84p4F+COo6ehgJ7w8Sp9 MlwSXiGwQlgxKpjFL0o8X2Q45yPZUaWrbUsW+i9g3ckLR/i8RD4V6gqgsi9+QRF/ XsPS9lGki6FV8UxobNzlKFmEtDN8EiqUVn3zVmBCD1Klu+/Dua9cI6N22oq/woVI kqhJN+2NCGPl72levNQkb2CsmZ1Pv6CLwrsaDaRXFEA3em736qDmdiQ5pmLq6SDo G1awzPayzUkWgrJKyAnz9SZZmYvQSPbNS3Sk/zHEbhIR/yDxn0DmoP/wuXud+YvR rdOr9bvtYYGfkpi4mWYJWsoueFV+v4991evZRAzxge/2kKn2ONyk/hMSGSWBtwNq QPWFzss2kNiE97SsZ3v8AhOAiPSdjmhh6WOudN8PPox0mx+ulI8UxuMxP3hZP2jS JomlbMvIlf1Jmuzt/I4K7lP61llvQKqsl89bhYLnQyTG7WOoyEL+VmGwyWN+XECX 8zvclFqwzNOOHCMXVfcynB4kZCWeX9L997N8ivj4SiqQ9w0YG2tFaQ6yQUnPMs27 5qBaAz7eQVuGM8Y65fs5S0m+DsJ/PvzfrPoXsKroxFhTrGqtvRkqVt7Z9FxFXctk 2OQO7lf8WgW5jGG/mqPxZ5Q4M7YoSAlkPKbcbCvcLsrVXTr+/ghwnFsGKCDCR4QR pjTqeMxFPHWVhzeSTAmZ0Kbk5i7KUug89TFxfilJa/v4bhEKDm6pcnSorWSUJlhN xxkehZ1GCJdOGNAvKwQ6Gl+2q/W0f+cMT9z2nYB/3Hx4xnqCL4EXRxq0SkoCQDVW W5uMaPy/un6jV+2qrgnt8Sn2Z9Pc0TVo/N3yXGb4mkRFRTn1IPiZePDSHsZ20oXr 4WC403Iw0gs3X5eM/s1DGdi00gU5As8vYScaMQXb68Th+fFPfvc9o9lVfeHkiM/L KcbdeGWnYuc8lj1AVSEubiHLoFGvePk8jsDaWE8oaV4IhLTsU5x3Baj5rNUfqMZv MhFsjpTpmdbgroekHpP1hqeXp8/0TJ/1o96ublAqC5LJpfbYOVGeP3OJgx/ASmrp RA3rnyTeLSAN2KOdvaSyZxGmCxYMLu9p -----END CERTIFICATE-----`,
			output: "-----BEGIN CERTIFICATE-----\n MIIFJDBWBgkqhkiG9w0BBQ0wSTAxBgkqhkiG9w0BBQwwJAQQ64IduEZCqZfR5Nix HyETFQICCAAwDAYIKoZIhvcNAgkFADAUBggqhkiG9w0DBwQILyNp24M/i6MEggTI xnr7+5ltJidFtjYBhUZJz0LxmQD+2PmH4D5a22lkewL22iLcdLuQbWdvGzXSaCmP IkhQ9k/KRrzda98IkUuZ3X3uqgvqeJjXh7nqCFahfTvZhfFaINkCqzowl03wB9Dn rKxIT9C7Z00m2CSdxL+cHSYAWl1GIn/d7j99bHWa/m7H5f3BuE/xStYJG1gHrC+g aB+dzZLDygiTYqc4SaTtHDBT1PeIJqiDQOgfKGF9vZKdtrfAjPlS2MI+s2AJHO3B h6zTMjhIrQ6RX/Y5gGm5DkVeuygP2unf8EBoOc5MOkeTb9hMqnKlBz2fumo/I5mu qo5DHk4EZpnqNymRaY0glp6bjd3vMj889kiZ2Rdtf4doieYuggwYBJ4WxpQ2GQAy RfGMcDOWHIwAe9p4+prgNet6Atvtp2YEH7LjXVvmNFTGbK6Jo+Fxp5n5B/3b54kB 6RFJa6JJz4YHHEU6w8woTanr/9W2urKncbQXQW5kUW2YBSCz3ZWHNr93AY9IVXa2 csAuAcQng/8coR2A20QJybntYLHUl1p0bZMM2XgaCeqD84p4F+COo6ehgJ7w8Sp9 MlwSXiGwQlgxKpjFL0o8X2Q45yPZUaWrbUsW+i9g3ckLR/i8RD4V6gqgsi9+QRF/ XsPS9lGki6FV8UxobNzlKFmEtDN8EiqUVn3zVmBCD1Klu+/Dua9cI6N22oq/woVI kqhJN+2NCGPl72levNQkb2CsmZ1Pv6CLwrsaDaRXFEA3em736qDmdiQ5pmLq6SDo G1awzPayzUkWgrJKyAnz9SZZmYvQSPbNS3Sk/zHEbhIR/yDxn0DmoP/wuXud+YvR rdOr9bvtYYGfkpi4mWYJWsoueFV+v4991evZRAzxge/2kKn2ONyk/hMSGSWBtwNq QPWFzss2kNiE97SsZ3v8AhOAiPSdjmhh6WOudN8PPox0mx+ulI8UxuMxP3hZP2jS JomlbMvIlf1Jmuzt/I4K7lP61llvQKqsl89bhYLnQyTG7WOoyEL+VmGwyWN+XECX 8zvclFqwzNOOHCMXVfcynB4kZCWeX9L997N8ivj4SiqQ9w0YG2tFaQ6yQUnPMs27 5qBaAz7eQVuGM8Y65fs5S0m+DsJ/PvzfrPoXsKroxFhTrGqtvRkqVt7Z9FxFXctk 2OQO7lf8WgW5jGG/mqPxZ5Q4M7YoSAlkPKbcbCvcLsrVXTr+/ghwnFsGKCDCR4QR pjTqeMxFPHWVhzeSTAmZ0Kbk5i7KUug89TFxfilJa/v4bhEKDm6pcnSorWSUJlhN xxkehZ1GCJdOGNAvKwQ6Gl+2q/W0f+cMT9z2nYB/3Hx4xnqCL4EXRxq0SkoCQDVW W5uMaPy/un6jV+2qrgnt8Sn2Z9Pc0TVo/N3yXGb4mkRFRTn1IPiZePDSHsZ20oXr 4WC403Iw0gs3X5eM/s1DGdi00gU5As8vYScaMQXb68Th+fFPfvc9o9lVfeHkiM/L KcbdeGWnYuc8lj1AVSEubiHLoFGvePk8jsDaWE8oaV4IhLTsU5x3Baj5rNUfqMZv MhFsjpTpmdbgroekHpP1hqeXp8/0TJ/1o96ublAqC5LJpfbYOVGeP3OJgx/ASmrp RA3rnyTeLSAN2KOdvaSyZxGmCxYMLu9p \n-----END CERTIFICATE-----\n",
		},
		{
			name:   "certificate (textArea)",
			input:  "-----BEGIN CERTIFICATE-----\nMIIFJDBWBgkqhkiG9w0BBQ0wSTAxBgkqhkiG9w0BBQwwJAQQpWee/aYJAeHHT9AS\npIo+jQICCAAwDAYIKoZIhvcNAgkFADAUBggqhkiG9w0DBwQInSbkcxgNEisEggTI\n/3+KEvhVubn3GXS/w0QvJz0qR/gjgWSZ5e+c8U5DmVAyjzftS/QNFIX8ArYDwFUh\nCy9wJEmbPRlcRloXTBsk5IMT0MYIa/4zGxfqPWfgdkxrJzS2sCQP+FwsgkSUEvYj\nI7UEJ8kxmfew30RCJRJlNdzYPg8HAYlVizyemWxhrnFT8HE4Len+ILJUN0HGfra6\nU8pLI6MKnGRqLZBWIhc+2JJ/UqWQexVClN/gNV3xkC5CM7CsRsDRJw7bbWFwH2Eo\n0VStFV3DVpjf++VnPoRlRi++3olXVxO1I2e+SR1fU0CVjzXE+Q+ltWJHiBsQ7kWt\nM7weOfvd1AxAYhM7HzHOyI5JyawaBUnc2PNqzrDv1AOU8HIOe1JCuvj4RWI//BpE\nsZmjjGBMRzTsorWMILaWFEnC5lefjd06Cmag5jsLoLrZeewqwix7+r8SYVptnl5O\njNO9lZU83HJwH5W9TPHB7OCQPOMGqjAnIeDEwLPjBWGdylyf/BZamvoONG74f5kq\n8I3bX4rxMM71vg6xWcS+MKKn/4ch3oIjuN+lUOVJH1G2wEROVzbQknWns6JM+Jsa\nfQjQh0YigdVwbHC99wCLtwVVXcmpA5Jj2z8wvqCYnb4MlaYt8Ld7hVaaF13tp9lM\nMazxIOIJmHuX4BB3wVFTCygHqKzEUczyMnjKfAKO1BL72ZYQAI9nbZmSUuXuvxun\nKPk6XqTxSJCjGHQxIFkEJVHT9qhxI5MUgdo6R+BVvPCxdo7Xnikw3DKij/BRlhWa\nDj+WSAXH0Xvln/GownUCVACOY10dkFkUEpmvV3cKbSMBwGnp0aagFGKaP33O6R75\nDLLzVv4/vhZQEIpUKjmwWNOYfZ5Yz5ndKJ6B3eFwYXoEQkCLiEOddP6A7Soeasss\nYV8jN00MUyFH9xTzvtIcsWeu5PYVcngE2vyGXkbrzWCs6vtaGQDNi7+HfzYGtKH4\njL+BHYwwxuSzn2ki1ondrtzP7+NNc6PUJfcs5/C0DwXK0ymAKlzEtxQk9infCMa2\n+hCbeO1RwyqWT/pDDruVJZ2r/IcPag1rrqSPVYPz19RVxV1Td2TLlex6Nwa6JE7z\np1cNpopxftrCz0Ajw4qIEJ9tP/ztZAiaf9dHHREKckMxSv41AcypVSIfpw5WTlwU\nsomn5mbBX/r1M6F43fjEUh4NCPNBb80xu1Z1jP9AZulh1O/6Fj6jQXZVVuEOFJaq\nyxIy4ocLA7/1VpchZ0RpbvQpq2/I2N4H+Reqk5oodMZf0APiV3d6v3iN1YbVL0aV\n8NODoCbs3IJBSaNgCjwfNyA4rtjBhup8doSJ/oTY30ZMX4uGZbpRLjJbUItK1IrD\n47Hh2Ga3FhgIblMj9Fg9GqrPvU2PMplrWdbxWcpuV7klvKAx4zzDxiZiPQNqkvrX\nELIqVf33GgggqmEqqNFXZDUXqSd5LIzsR7pEnaksIQ46jhQtP8WZLpkKlWTaDX2E\nvYQMhz9A0NT2hONOA9aLUCiyLvYjrYR9r7hhj5fpEDjOi8rIs/+NW/wrZhsJodPt\nWVJXx3MgHkN8tzJ40kEKBQlViQXxh2bSQjjP8WePRHX6rMmvIzWaJcOZk+lfrUGn\nVd2oqQJsSntAE0KdZZSZCBTkx39xJEVS\n-----END CERTIFICATE-----",
			output: "-----BEGIN CERTIFICATE-----\n MIIFJDBWBgkqhkiG9w0BBQ0wSTAxBgkqhkiG9w0BBQwwJAQQpWee/aYJAeHHT9AS pIo+jQICCAAwDAYIKoZIhvcNAgkFADAUBggqhkiG9w0DBwQInSbkcxgNEisEggTI /3+KEvhVubn3GXS/w0QvJz0qR/gjgWSZ5e+c8U5DmVAyjzftS/QNFIX8ArYDwFUh Cy9wJEmbPRlcRloXTBsk5IMT0MYIa/4zGxfqPWfgdkxrJzS2sCQP+FwsgkSUEvYj I7UEJ8kxmfew30RCJRJlNdzYPg8HAYlVizyemWxhrnFT8HE4Len+ILJUN0HGfra6 U8pLI6MKnGRqLZBWIhc+2JJ/UqWQexVClN/gNV3xkC5CM7CsRsDRJw7bbWFwH2Eo 0VStFV3DVpjf++VnPoRlRi++3olXVxO1I2e+SR1fU0CVjzXE+Q+ltWJHiBsQ7kWt M7weOfvd1AxAYhM7HzHOyI5JyawaBUnc2PNqzrDv1AOU8HIOe1JCuvj4RWI//BpE sZmjjGBMRzTsorWMILaWFEnC5lefjd06Cmag5jsLoLrZeewqwix7+r8SYVptnl5O jNO9lZU83HJwH5W9TPHB7OCQPOMGqjAnIeDEwLPjBWGdylyf/BZamvoONG74f5kq 8I3bX4rxMM71vg6xWcS+MKKn/4ch3oIjuN+lUOVJH1G2wEROVzbQknWns6JM+Jsa fQjQh0YigdVwbHC99wCLtwVVXcmpA5Jj2z8wvqCYnb4MlaYt8Ld7hVaaF13tp9lM MazxIOIJmHuX4BB3wVFTCygHqKzEUczyMnjKfAKO1BL72ZYQAI9nbZmSUuXuvxun KPk6XqTxSJCjGHQxIFkEJVHT9qhxI5MUgdo6R+BVvPCxdo7Xnikw3DKij/BRlhWa Dj+WSAXH0Xvln/GownUCVACOY10dkFkUEpmvV3cKbSMBwGnp0aagFGKaP33O6R75 DLLzVv4/vhZQEIpUKjmwWNOYfZ5Yz5ndKJ6B3eFwYXoEQkCLiEOddP6A7Soeasss YV8jN00MUyFH9xTzvtIcsWeu5PYVcngE2vyGXkbrzWCs6vtaGQDNi7+HfzYGtKH4 jL+BHYwwxuSzn2ki1ondrtzP7+NNc6PUJfcs5/C0DwXK0ymAKlzEtxQk9infCMa2 +hCbeO1RwyqWT/pDDruVJZ2r/IcPag1rrqSPVYPz19RVxV1Td2TLlex6Nwa6JE7z p1cNpopxftrCz0Ajw4qIEJ9tP/ztZAiaf9dHHREKckMxSv41AcypVSIfpw5WTlwU somn5mbBX/r1M6F43fjEUh4NCPNBb80xu1Z1jP9AZulh1O/6Fj6jQXZVVuEOFJaq yxIy4ocLA7/1VpchZ0RpbvQpq2/I2N4H+Reqk5oodMZf0APiV3d6v3iN1YbVL0aV 8NODoCbs3IJBSaNgCjwfNyA4rtjBhup8doSJ/oTY30ZMX4uGZbpRLjJbUItK1IrD 47Hh2Ga3FhgIblMj9Fg9GqrPvU2PMplrWdbxWcpuV7klvKAx4zzDxiZiPQNqkvrX ELIqVf33GgggqmEqqNFXZDUXqSd5LIzsR7pEnaksIQ46jhQtP8WZLpkKlWTaDX2E vYQMhz9A0NT2hONOA9aLUCiyLvYjrYR9r7hhj5fpEDjOi8rIs/+NW/wrZhsJodPt WVJXx3MgHkN8tzJ40kEKBQlViQXxh2bSQjjP8WePRHX6rMmvIzWaJcOZk+lfrUGn Vd2oqQJsSntAE0KdZZSZCBTkx39xJEVS \n-----END CERTIFICATE-----\n",
		},
		{
			name:   "rsa private key (textInput)",
			input:  `-----BEGIN RSA PRIVATE KEY----- MIIFJDBWBgkqhkiG9w0BBQ0wSTAxBgkqhkiG9w0BBQwwJAQQ64IduEZCqZfR5Nix HyETFQICCAAwDAYIKoZIhvcNAgkFADAUBggqhkiG9w0DBwQILyNp24M/i6MEggTI xnr7+5ltJidFtjYBhUZJz0LxmQD+2PmH4D5a22lkewL22iLcdLuQbWdvGzXSaCmP IkhQ9k/KRrzda98IkUuZ3X3uqgvqeJjXh7nqCFahfTvZhfFaINkCqzowl03wB9Dn rKxIT9C7Z00m2CSdxL+cHSYAWl1GIn/d7j99bHWa/m7H5f3BuE/xStYJG1gHrC+g aB+dzZLDygiTYqc4SaTtHDBT1PeIJqiDQOgfKGF9vZKdtrfAjPlS2MI+s2AJHO3B h6zTMjhIrQ6RX/Y5gGm5DkVeuygP2unf8EBoOc5MOkeTb9hMqnKlBz2fumo/I5mu qo5DHk4EZpnqNymRaY0glp6bjd3vMj889kiZ2Rdtf4doieYuggwYBJ4WxpQ2GQAy RfGMcDOWHIwAe9p4+prgNet6Atvtp2YEH7LjXVvmNFTGbK6Jo+Fxp5n5B/3b54kB 6RFJa6JJz4YHHEU6w8woTanr/9W2urKncbQXQW5kUW2YBSCz3ZWHNr93AY9IVXa2 csAuAcQng/8coR2A20QJybntYLHUl1p0bZMM2XgaCeqD84p4F+COo6ehgJ7w8Sp9 MlwSXiGwQlgxKpjFL0o8X2Q45yPZUaWrbUsW+i9g3ckLR/i8RD4V6gqgsi9+QRF/ XsPS9lGki6FV8UxobNzlKFmEtDN8EiqUVn3zVmBCD1Klu+/Dua9cI6N22oq/woVI kqhJN+2NCGPl72levNQkb2CsmZ1Pv6CLwrsaDaRXFEA3em736qDmdiQ5pmLq6SDo G1awzPayzUkWgrJKyAnz9SZZmYvQSPbNS3Sk/zHEbhIR/yDxn0DmoP/wuXud+YvR rdOr9bvtYYGfkpi4mWYJWsoueFV+v4991evZRAzxge/2kKn2ONyk/hMSGSWBtwNq QPWFzss2kNiE97SsZ3v8AhOAiPSdjmhh6WOudN8PPox0mx+ulI8UxuMxP3hZP2jS JomlbMvIlf1Jmuzt/I4K7lP61llvQKqsl89bhYLnQyTG7WOoyEL+VmGwyWN+XECX 8zvclFqwzNOOHCMXVfcynB4kZCWeX9L997N8ivj4SiqQ9w0YG2tFaQ6yQUnPMs27 5qBaAz7eQVuGM8Y65fs5S0m+DsJ/PvzfrPoXsKroxFhTrGqtvRkqVt7Z9FxFXctk 2OQO7lf8WgW5jGG/mqPxZ5Q4M7YoSAlkPKbcbCvcLsrVXTr+/ghwnFsGKCDCR4QR pjTqeMxFPHWVhzeSTAmZ0Kbk5i7KUug89TFxfilJa/v4bhEKDm6pcnSorWSUJlhN xxkehZ1GCJdOGNAvKwQ6Gl+2q/W0f+cMT9z2nYB/3Hx4xnqCL4EXRxq0SkoCQDVW W5uMaPy/un6jV+2qrgnt8Sn2Z9Pc0TVo/N3yXGb4mkRFRTn1IPiZePDSHsZ20oXr 4WC403Iw0gs3X5eM/s1DGdi00gU5As8vYScaMQXb68Th+fFPfvc9o9lVfeHkiM/L KcbdeGWnYuc8lj1AVSEubiHLoFGvePk8jsDaWE8oaV4IhLTsU5x3Baj5rNUfqMZv MhFsjpTpmdbgroekHpP1hqeXp8/0TJ/1o96ublAqC5LJpfbYOVGeP3OJgx/ASmrp RA3rnyTeLSAN2KOdvaSyZxGmCxYMLu9p -----END RSA PRIVATE KEY-----`,
			output: "-----BEGIN RSA PRIVATE KEY-----\n MIIFJDBWBgkqhkiG9w0BBQ0wSTAxBgkqhkiG9w0BBQwwJAQQ64IduEZCqZfR5Nix HyETFQICCAAwDAYIKoZIhvcNAgkFADAUBggqhkiG9w0DBwQILyNp24M/i6MEggTI xnr7+5ltJidFtjYBhUZJz0LxmQD+2PmH4D5a22lkewL22iLcdLuQbWdvGzXSaCmP IkhQ9k/KRrzda98IkUuZ3X3uqgvqeJjXh7nqCFahfTvZhfFaINkCqzowl03wB9Dn rKxIT9C7Z00m2CSdxL+cHSYAWl1GIn/d7j99bHWa/m7H5f3BuE/xStYJG1gHrC+g aB+dzZLDygiTYqc4SaTtHDBT1PeIJqiDQOgfKGF9vZKdtrfAjPlS2MI+s2AJHO3B h6zTMjhIrQ6RX/Y5gGm5DkVeuygP2unf8EBoOc5MOkeTb9hMqnKlBz2fumo/I5mu qo5DHk4EZpnqNymRaY0glp6bjd3vMj889kiZ2Rdtf4doieYuggwYBJ4WxpQ2GQAy RfGMcDOWHIwAe9p4+prgNet6Atvtp2YEH7LjXVvmNFTGbK6Jo+Fxp5n5B/3b54kB 6RFJa6JJz4YHHEU6w8woTanr/9W2urKncbQXQW5kUW2YBSCz3ZWHNr93AY9IVXa2 csAuAcQng/8coR2A20QJybntYLHUl1p0bZMM2XgaCeqD84p4F+COo6ehgJ7w8Sp9 MlwSXiGwQlgxKpjFL0o8X2Q45yPZUaWrbUsW+i9g3ckLR/i8RD4V6gqgsi9+QRF/ XsPS9lGki6FV8UxobNzlKFmEtDN8EiqUVn3zVmBCD1Klu+/Dua9cI6N22oq/woVI kqhJN+2NCGPl72levNQkb2CsmZ1Pv6CLwrsaDaRXFEA3em736qDmdiQ5pmLq6SDo G1awzPayzUkWgrJKyAnz9SZZmYvQSPbNS3Sk/zHEbhIR/yDxn0DmoP/wuXud+YvR rdOr9bvtYYGfkpi4mWYJWsoueFV+v4991evZRAzxge/2kKn2ONyk/hMSGSWBtwNq QPWFzss2kNiE97SsZ3v8AhOAiPSdjmhh6WOudN8PPox0mx+ulI8UxuMxP3hZP2jS JomlbMvIlf1Jmuzt/I4K7lP61llvQKqsl89bhYLnQyTG7WOoyEL+VmGwyWN+XECX 8zvclFqwzNOOHCMXVfcynB4kZCWeX9L997N8ivj4SiqQ9w0YG2tFaQ6yQUnPMs27 5qBaAz7eQVuGM8Y65fs5S0m+DsJ/PvzfrPoXsKroxFhTrGqtvRkqVt7Z9FxFXctk 2OQO7lf8WgW5jGG/mqPxZ5Q4M7YoSAlkPKbcbCvcLsrVXTr+/ghwnFsGKCDCR4QR pjTqeMxFPHWVhzeSTAmZ0Kbk5i7KUug89TFxfilJa/v4bhEKDm6pcnSorWSUJlhN xxkehZ1GCJdOGNAvKwQ6Gl+2q/W0f+cMT9z2nYB/3Hx4xnqCL4EXRxq0SkoCQDVW W5uMaPy/un6jV+2qrgnt8Sn2Z9Pc0TVo/N3yXGb4mkRFRTn1IPiZePDSHsZ20oXr 4WC403Iw0gs3X5eM/s1DGdi00gU5As8vYScaMQXb68Th+fFPfvc9o9lVfeHkiM/L KcbdeGWnYuc8lj1AVSEubiHLoFGvePk8jsDaWE8oaV4IhLTsU5x3Baj5rNUfqMZv MhFsjpTpmdbgroekHpP1hqeXp8/0TJ/1o96ublAqC5LJpfbYOVGeP3OJgx/ASmrp RA3rnyTeLSAN2KOdvaSyZxGmCxYMLu9p \n-----END RSA PRIVATE KEY-----\n",
		},
		{
			name:   "rsa private key (textArea)",
			input:  "-----BEGIN RSA PRIVATE KEY-----\nMIIFJDBWBgkqhkiG9w0BBQ0wSTAxBgkqhkiG9w0BBQwwJAQQpWee/aYJAeHHT9AS\npIo+jQICCAAwDAYIKoZIhvcNAgkFADAUBggqhkiG9w0DBwQInSbkcxgNEisEggTI\n/3+KEvhVubn3GXS/w0QvJz0qR/gjgWSZ5e+c8U5DmVAyjzftS/QNFIX8ArYDwFUh\nCy9wJEmbPRlcRloXTBsk5IMT0MYIa/4zGxfqPWfgdkxrJzS2sCQP+FwsgkSUEvYj\nI7UEJ8kxmfew30RCJRJlNdzYPg8HAYlVizyemWxhrnFT8HE4Len+ILJUN0HGfra6\nU8pLI6MKnGRqLZBWIhc+2JJ/UqWQexVClN/gNV3xkC5CM7CsRsDRJw7bbWFwH2Eo\n0VStFV3DVpjf++VnPoRlRi++3olXVxO1I2e+SR1fU0CVjzXE+Q+ltWJHiBsQ7kWt\nM7weOfvd1AxAYhM7HzHOyI5JyawaBUnc2PNqzrDv1AOU8HIOe1JCuvj4RWI//BpE\nsZmjjGBMRzTsorWMILaWFEnC5lefjd06Cmag5jsLoLrZeewqwix7+r8SYVptnl5O\njNO9lZU83HJwH5W9TPHB7OCQPOMGqjAnIeDEwLPjBWGdylyf/BZamvoONG74f5kq\n8I3bX4rxMM71vg6xWcS+MKKn/4ch3oIjuN+lUOVJH1G2wEROVzbQknWns6JM+Jsa\nfQjQh0YigdVwbHC99wCLtwVVXcmpA5Jj2z8wvqCYnb4MlaYt8Ld7hVaaF13tp9lM\nMazxIOIJmHuX4BB3wVFTCygHqKzEUczyMnjKfAKO1BL72ZYQAI9nbZmSUuXuvxun\nKPk6XqTxSJCjGHQxIFkEJVHT9qhxI5MUgdo6R+BVvPCxdo7Xnikw3DKij/BRlhWa\nDj+WSAXH0Xvln/GownUCVACOY10dkFkUEpmvV3cKbSMBwGnp0aagFGKaP33O6R75\nDLLzVv4/vhZQEIpUKjmwWNOYfZ5Yz5ndKJ6B3eFwYXoEQkCLiEOddP6A7Soeasss\nYV8jN00MUyFH9xTzvtIcsWeu5PYVcngE2vyGXkbrzWCs6vtaGQDNi7+HfzYGtKH4\njL+BHYwwxuSzn2ki1ondrtzP7+NNc6PUJfcs5/C0DwXK0ymAKlzEtxQk9infCMa2\n+hCbeO1RwyqWT/pDDruVJZ2r/IcPag1rrqSPVYPz19RVxV1Td2TLlex6Nwa6JE7z\np1cNpopxftrCz0Ajw4qIEJ9tP/ztZAiaf9dHHREKckMxSv41AcypVSIfpw5WTlwU\nsomn5mbBX/r1M6F43fjEUh4NCPNBb80xu1Z1jP9AZulh1O/6Fj6jQXZVVuEOFJaq\nyxIy4ocLA7/1VpchZ0RpbvQpq2/I2N4H+Reqk5oodMZf0APiV3d6v3iN1YbVL0aV\n8NODoCbs3IJBSaNgCjwfNyA4rtjBhup8doSJ/oTY30ZMX4uGZbpRLjJbUItK1IrD\n47Hh2Ga3FhgIblMj9Fg9GqrPvU2PMplrWdbxWcpuV7klvKAx4zzDxiZiPQNqkvrX\nELIqVf33GgggqmEqqNFXZDUXqSd5LIzsR7pEnaksIQ46jhQtP8WZLpkKlWTaDX2E\nvYQMhz9A0NT2hONOA9aLUCiyLvYjrYR9r7hhj5fpEDjOi8rIs/+NW/wrZhsJodPt\nWVJXx3MgHkN8tzJ40kEKBQlViQXxh2bSQjjP8WePRHX6rMmvIzWaJcOZk+lfrUGn\nVd2oqQJsSntAE0KdZZSZCBTkx39xJEVS\n-----END RSA PRIVATE KEY-----",
			output: "-----BEGIN RSA PRIVATE KEY-----\n MIIFJDBWBgkqhkiG9w0BBQ0wSTAxBgkqhkiG9w0BBQwwJAQQpWee/aYJAeHHT9AS pIo+jQICCAAwDAYIKoZIhvcNAgkFADAUBggqhkiG9w0DBwQInSbkcxgNEisEggTI /3+KEvhVubn3GXS/w0QvJz0qR/gjgWSZ5e+c8U5DmVAyjzftS/QNFIX8ArYDwFUh Cy9wJEmbPRlcRloXTBsk5IMT0MYIa/4zGxfqPWfgdkxrJzS2sCQP+FwsgkSUEvYj I7UEJ8kxmfew30RCJRJlNdzYPg8HAYlVizyemWxhrnFT8HE4Len+ILJUN0HGfra6 U8pLI6MKnGRqLZBWIhc+2JJ/UqWQexVClN/gNV3xkC5CM7CsRsDRJw7bbWFwH2Eo 0VStFV3DVpjf++VnPoRlRi++3olXVxO1I2e+SR1fU0CVjzXE+Q+ltWJHiBsQ7kWt M7weOfvd1AxAYhM7HzHOyI5JyawaBUnc2PNqzrDv1AOU8HIOe1JCuvj4RWI//BpE sZmjjGBMRzTsorWMILaWFEnC5lefjd06Cmag5jsLoLrZeewqwix7+r8SYVptnl5O jNO9lZU83HJwH5W9TPHB7OCQPOMGqjAnIeDEwLPjBWGdylyf/BZamvoONG74f5kq 8I3bX4rxMM71vg6xWcS+MKKn/4ch3oIjuN+lUOVJH1G2wEROVzbQknWns6JM+Jsa fQjQh0YigdVwbHC99wCLtwVVXcmpA5Jj2z8wvqCYnb4MlaYt8Ld7hVaaF13tp9lM MazxIOIJmHuX4BB3wVFTCygHqKzEUczyMnjKfAKO1BL72ZYQAI9nbZmSUuXuvxun KPk6XqTxSJCjGHQxIFkEJVHT9qhxI5MUgdo6R+BVvPCxdo7Xnikw3DKij/BRlhWa Dj+WSAXH0Xvln/GownUCVACOY10dkFkUEpmvV3cKbSMBwGnp0aagFGKaP33O6R75 DLLzVv4/vhZQEIpUKjmwWNOYfZ5Yz5ndKJ6B3eFwYXoEQkCLiEOddP6A7Soeasss YV8jN00MUyFH9xTzvtIcsWeu5PYVcngE2vyGXkbrzWCs6vtaGQDNi7+HfzYGtKH4 jL+BHYwwxuSzn2ki1ondrtzP7+NNc6PUJfcs5/C0DwXK0ymAKlzEtxQk9infCMa2 +hCbeO1RwyqWT/pDDruVJZ2r/IcPag1rrqSPVYPz19RVxV1Td2TLlex6Nwa6JE7z p1cNpopxftrCz0Ajw4qIEJ9tP/ztZAiaf9dHHREKckMxSv41AcypVSIfpw5WTlwU somn5mbBX/r1M6F43fjEUh4NCPNBb80xu1Z1jP9AZulh1O/6Fj6jQXZVVuEOFJaq yxIy4ocLA7/1VpchZ0RpbvQpq2/I2N4H+Reqk5oodMZf0APiV3d6v3iN1YbVL0aV 8NODoCbs3IJBSaNgCjwfNyA4rtjBhup8doSJ/oTY30ZMX4uGZbpRLjJbUItK1IrD 47Hh2Ga3FhgIblMj9Fg9GqrPvU2PMplrWdbxWcpuV7klvKAx4zzDxiZiPQNqkvrX ELIqVf33GgggqmEqqNFXZDUXqSd5LIzsR7pEnaksIQ46jhQtP8WZLpkKlWTaDX2E vYQMhz9A0NT2hONOA9aLUCiyLvYjrYR9r7hhj5fpEDjOi8rIs/+NW/wrZhsJodPt WVJXx3MgHkN8tzJ40kEKBQlViQXxh2bSQjjP8WePRHX6rMmvIzWaJcOZk+lfrUGn Vd2oqQJsSntAE0KdZZSZCBTkx39xJEVS \n-----END RSA PRIVATE KEY-----\n",
		},
		{
			name:   "encrypted private key (textInput)",
			input:  `-----BEGIN ENCRYPTED PRIVATE KEY----- MIIFJDBWBgkqhkiG9w0BBQ0wSTAxBgkqhkiG9w0BBQwwJAQQ64IduEZCqZfR5Nix HyETFQICCAAwDAYIKoZIhvcNAgkFADAUBggqhkiG9w0DBwQILyNp24M/i6MEggTI xnr7+5ltJidFtjYBhUZJz0LxmQD+2PmH4D5a22lkewL22iLcdLuQbWdvGzXSaCmP IkhQ9k/KRrzda98IkUuZ3X3uqgvqeJjXh7nqCFahfTvZhfFaINkCqzowl03wB9Dn rKxIT9C7Z00m2CSdxL+cHSYAWl1GIn/d7j99bHWa/m7H5f3BuE/xStYJG1gHrC+g aB+dzZLDygiTYqc4SaTtHDBT1PeIJqiDQOgfKGF9vZKdtrfAjPlS2MI+s2AJHO3B h6zTMjhIrQ6RX/Y5gGm5DkVeuygP2unf8EBoOc5MOkeTb9hMqnKlBz2fumo/I5mu qo5DHk4EZpnqNymRaY0glp6bjd3vMj889kiZ2Rdtf4doieYuggwYBJ4WxpQ2GQAy RfGMcDOWHIwAe9p4+prgNet6Atvtp2YEH7LjXVvmNFTGbK6Jo+Fxp5n5B/3b54kB 6RFJa6JJz4YHHEU6w8woTanr/9W2urKncbQXQW5kUW2YBSCz3ZWHNr93AY9IVXa2 csAuAcQng/8coR2A20QJybntYLHUl1p0bZMM2XgaCeqD84p4F+COo6ehgJ7w8Sp9 MlwSXiGwQlgxKpjFL0o8X2Q45yPZUaWrbUsW+i9g3ckLR/i8RD4V6gqgsi9+QRF/ XsPS9lGki6FV8UxobNzlKFmEtDN8EiqUVn3zVmBCD1Klu+/Dua9cI6N22oq/woVI kqhJN+2NCGPl72levNQkb2CsmZ1Pv6CLwrsaDaRXFEA3em736qDmdiQ5pmLq6SDo G1awzPayzUkWgrJKyAnz9SZZmYvQSPbNS3Sk/zHEbhIR/yDxn0DmoP/wuXud+YvR rdOr9bvtYYGfkpi4mWYJWsoueFV+v4991evZRAzxge/2kKn2ONyk/hMSGSWBtwNq QPWFzss2kNiE97SsZ3v8AhOAiPSdjmhh6WOudN8PPox0mx+ulI8UxuMxP3hZP2jS JomlbMvIlf1Jmuzt/I4K7lP61llvQKqsl89bhYLnQyTG7WOoyEL+VmGwyWN+XECX 8zvclFqwzNOOHCMXVfcynB4kZCWeX9L997N8ivj4SiqQ9w0YG2tFaQ6yQUnPMs27 5qBaAz7eQVuGM8Y65fs5S0m+DsJ/PvzfrPoXsKroxFhTrGqtvRkqVt7Z9FxFXctk 2OQO7lf8WgW5jGG/mqPxZ5Q4M7YoSAlkPKbcbCvcLsrVXTr+/ghwnFsGKCDCR4QR pjTqeMxFPHWVhzeSTAmZ0Kbk5i7KUug89TFxfilJa/v4bhEKDm6pcnSorWSUJlhN xxkehZ1GCJdOGNAvKwQ6Gl+2q/W0f+cMT9z2nYB/3Hx4xnqCL4EXRxq0SkoCQDVW W5uMaPy/un6jV+2qrgnt8Sn2Z9Pc0TVo/N3yXGb4mkRFRTn1IPiZePDSHsZ20oXr 4WC403Iw0gs3X5eM/s1DGdi00gU5As8vYScaMQXb68Th+fFPfvc9o9lVfeHkiM/L KcbdeGWnYuc8lj1AVSEubiHLoFGvePk8jsDaWE8oaV4IhLTsU5x3Baj5rNUfqMZv MhFsjpTpmdbgroekHpP1hqeXp8/0TJ/1o96ublAqC5LJpfbYOVGeP3OJgx/ASmrp RA3rnyTeLSAN2KOdvaSyZxGmCxYMLu9p -----END ENCRYPTED PRIVATE KEY-----`,
			output: "-----BEGIN ENCRYPTED PRIVATE KEY-----\n MIIFJDBWBgkqhkiG9w0BBQ0wSTAxBgkqhkiG9w0BBQwwJAQQ64IduEZCqZfR5Nix HyETFQICCAAwDAYIKoZIhvcNAgkFADAUBggqhkiG9w0DBwQILyNp24M/i6MEggTI xnr7+5ltJidFtjYBhUZJz0LxmQD+2PmH4D5a22lkewL22iLcdLuQbWdvGzXSaCmP IkhQ9k/KRrzda98IkUuZ3X3uqgvqeJjXh7nqCFahfTvZhfFaINkCqzowl03wB9Dn rKxIT9C7Z00m2CSdxL+cHSYAWl1GIn/d7j99bHWa/m7H5f3BuE/xStYJG1gHrC+g aB+dzZLDygiTYqc4SaTtHDBT1PeIJqiDQOgfKGF9vZKdtrfAjPlS2MI+s2AJHO3B h6zTMjhIrQ6RX/Y5gGm5DkVeuygP2unf8EBoOc5MOkeTb9hMqnKlBz2fumo/I5mu qo5DHk4EZpnqNymRaY0glp6bjd3vMj889kiZ2Rdtf4doieYuggwYBJ4WxpQ2GQAy RfGMcDOWHIwAe9p4+prgNet6Atvtp2YEH7LjXVvmNFTGbK6Jo+Fxp5n5B/3b54kB 6RFJa6JJz4YHHEU6w8woTanr/9W2urKncbQXQW5kUW2YBSCz3ZWHNr93AY9IVXa2 csAuAcQng/8coR2A20QJybntYLHUl1p0bZMM2XgaCeqD84p4F+COo6ehgJ7w8Sp9 MlwSXiGwQlgxKpjFL0o8X2Q45yPZUaWrbUsW+i9g3ckLR/i8RD4V6gqgsi9+QRF/ XsPS9lGki6FV8UxobNzlKFmEtDN8EiqUVn3zVmBCD1Klu+/Dua9cI6N22oq/woVI kqhJN+2NCGPl72levNQkb2CsmZ1Pv6CLwrsaDaRXFEA3em736qDmdiQ5pmLq6SDo G1awzPayzUkWgrJKyAnz9SZZmYvQSPbNS3Sk/zHEbhIR/yDxn0DmoP/wuXud+YvR rdOr9bvtYYGfkpi4mWYJWsoueFV+v4991evZRAzxge/2kKn2ONyk/hMSGSWBtwNq QPWFzss2kNiE97SsZ3v8AhOAiPSdjmhh6WOudN8PPox0mx+ulI8UxuMxP3hZP2jS JomlbMvIlf1Jmuzt/I4K7lP61llvQKqsl89bhYLnQyTG7WOoyEL+VmGwyWN+XECX 8zvclFqwzNOOHCMXVfcynB4kZCWeX9L997N8ivj4SiqQ9w0YG2tFaQ6yQUnPMs27 5qBaAz7eQVuGM8Y65fs5S0m+DsJ/PvzfrPoXsKroxFhTrGqtvRkqVt7Z9FxFXctk 2OQO7lf8WgW5jGG/mqPxZ5Q4M7YoSAlkPKbcbCvcLsrVXTr+/ghwnFsGKCDCR4QR pjTqeMxFPHWVhzeSTAmZ0Kbk5i7KUug89TFxfilJa/v4bhEKDm6pcnSorWSUJlhN xxkehZ1GCJdOGNAvKwQ6Gl+2q/W0f+cMT9z2nYB/3Hx4xnqCL4EXRxq0SkoCQDVW W5uMaPy/un6jV+2qrgnt8Sn2Z9Pc0TVo/N3yXGb4mkRFRTn1IPiZePDSHsZ20oXr 4WC403Iw0gs3X5eM/s1DGdi00gU5As8vYScaMQXb68Th+fFPfvc9o9lVfeHkiM/L KcbdeGWnYuc8lj1AVSEubiHLoFGvePk8jsDaWE8oaV4IhLTsU5x3Baj5rNUfqMZv MhFsjpTpmdbgroekHpP1hqeXp8/0TJ/1o96ublAqC5LJpfbYOVGeP3OJgx/ASmrp RA3rnyTeLSAN2KOdvaSyZxGmCxYMLu9p \n-----END ENCRYPTED PRIVATE KEY-----\n",
		},
		{
			name:   "encrypted private key (textArea)",
			input:  "-----BEGIN ENCRYPTED PRIVATE KEY-----\nMIIFJDBWBgkqhkiG9w0BBQ0wSTAxBgkqhkiG9w0BBQwwJAQQpWee/aYJAeHHT9AS\npIo+jQICCAAwDAYIKoZIhvcNAgkFADAUBggqhkiG9w0DBwQInSbkcxgNEisEggTI\n/3+KEvhVubn3GXS/w0QvJz0qR/gjgWSZ5e+c8U5DmVAyjzftS/QNFIX8ArYDwFUh\nCy9wJEmbPRlcRloXTBsk5IMT0MYIa/4zGxfqPWfgdkxrJzS2sCQP+FwsgkSUEvYj\nI7UEJ8kxmfew30RCJRJlNdzYPg8HAYlVizyemWxhrnFT8HE4Len+ILJUN0HGfra6\nU8pLI6MKnGRqLZBWIhc+2JJ/UqWQexVClN/gNV3xkC5CM7CsRsDRJw7bbWFwH2Eo\n0VStFV3DVpjf++VnPoRlRi++3olXVxO1I2e+SR1fU0CVjzXE+Q+ltWJHiBsQ7kWt\nM7weOfvd1AxAYhM7HzHOyI5JyawaBUnc2PNqzrDv1AOU8HIOe1JCuvj4RWI//BpE\nsZmjjGBMRzTsorWMILaWFEnC5lefjd06Cmag5jsLoLrZeewqwix7+r8SYVptnl5O\njNO9lZU83HJwH5W9TPHB7OCQPOMGqjAnIeDEwLPjBWGdylyf/BZamvoONG74f5kq\n8I3bX4rxMM71vg6xWcS+MKKn/4ch3oIjuN+lUOVJH1G2wEROVzbQknWns6JM+Jsa\nfQjQh0YigdVwbHC99wCLtwVVXcmpA5Jj2z8wvqCYnb4MlaYt8Ld7hVaaF13tp9lM\nMazxIOIJmHuX4BB3wVFTCygHqKzEUczyMnjKfAKO1BL72ZYQAI9nbZmSUuXuvxun\nKPk6XqTxSJCjGHQxIFkEJVHT9qhxI5MUgdo6R+BVvPCxdo7Xnikw3DKij/BRlhWa\nDj+WSAXH0Xvln/GownUCVACOY10dkFkUEpmvV3cKbSMBwGnp0aagFGKaP33O6R75\nDLLzVv4/vhZQEIpUKjmwWNOYfZ5Yz5ndKJ6B3eFwYXoEQkCLiEOddP6A7Soeasss\nYV8jN00MUyFH9xTzvtIcsWeu5PYVcngE2vyGXkbrzWCs6vtaGQDNi7+HfzYGtKH4\njL+BHYwwxuSzn2ki1ondrtzP7+NNc6PUJfcs5/C0DwXK0ymAKlzEtxQk9infCMa2\n+hCbeO1RwyqWT/pDDruVJZ2r/IcPag1rrqSPVYPz19RVxV1Td2TLlex6Nwa6JE7z\np1cNpopxftrCz0Ajw4qIEJ9tP/ztZAiaf9dHHREKckMxSv41AcypVSIfpw5WTlwU\nsomn5mbBX/r1M6F43fjEUh4NCPNBb80xu1Z1jP9AZulh1O/6Fj6jQXZVVuEOFJaq\nyxIy4ocLA7/1VpchZ0RpbvQpq2/I2N4H+Reqk5oodMZf0APiV3d6v3iN1YbVL0aV\n8NODoCbs3IJBSaNgCjwfNyA4rtjBhup8doSJ/oTY30ZMX4uGZbpRLjJbUItK1IrD\n47Hh2Ga3FhgIblMj9Fg9GqrPvU2PMplrWdbxWcpuV7klvKAx4zzDxiZiPQNqkvrX\nELIqVf33GgggqmEqqNFXZDUXqSd5LIzsR7pEnaksIQ46jhQtP8WZLpkKlWTaDX2E\nvYQMhz9A0NT2hONOA9aLUCiyLvYjrYR9r7hhj5fpEDjOi8rIs/+NW/wrZhsJodPt\nWVJXx3MgHkN8tzJ40kEKBQlViQXxh2bSQjjP8WePRHX6rMmvIzWaJcOZk+lfrUGn\nVd2oqQJsSntAE0KdZZSZCBTkx39xJEVS\n-----END ENCRYPTED PRIVATE KEY-----",
			output: "-----BEGIN ENCRYPTED PRIVATE KEY-----\n MIIFJDBWBgkqhkiG9w0BBQ0wSTAxBgkqhkiG9w0BBQwwJAQQpWee/aYJAeHHT9AS pIo+jQICCAAwDAYIKoZIhvcNAgkFADAUBggqhkiG9w0DBwQInSbkcxgNEisEggTI /3+KEvhVubn3GXS/w0QvJz0qR/gjgWSZ5e+c8U5DmVAyjzftS/QNFIX8ArYDwFUh Cy9wJEmbPRlcRloXTBsk5IMT0MYIa/4zGxfqPWfgdkxrJzS2sCQP+FwsgkSUEvYj I7UEJ8kxmfew30RCJRJlNdzYPg8HAYlVizyemWxhrnFT8HE4Len+ILJUN0HGfra6 U8pLI6MKnGRqLZBWIhc+2JJ/UqWQexVClN/gNV3xkC5CM7CsRsDRJw7bbWFwH2Eo 0VStFV3DVpjf++VnPoRlRi++3olXVxO1I2e+SR1fU0CVjzXE+Q+ltWJHiBsQ7kWt M7weOfvd1AxAYhM7HzHOyI5JyawaBUnc2PNqzrDv1AOU8HIOe1JCuvj4RWI//BpE sZmjjGBMRzTsorWMILaWFEnC5lefjd06Cmag5jsLoLrZeewqwix7+r8SYVptnl5O jNO9lZU83HJwH5W9TPHB7OCQPOMGqjAnIeDEwLPjBWGdylyf/BZamvoONG74f5kq 8I3bX4rxMM71vg6xWcS+MKKn/4ch3oIjuN+lUOVJH1G2wEROVzbQknWns6JM+Jsa fQjQh0YigdVwbHC99wCLtwVVXcmpA5Jj2z8wvqCYnb4MlaYt8Ld7hVaaF13tp9lM MazxIOIJmHuX4BB3wVFTCygHqKzEUczyMnjKfAKO1BL72ZYQAI9nbZmSUuXuvxun KPk6XqTxSJCjGHQxIFkEJVHT9qhxI5MUgdo6R+BVvPCxdo7Xnikw3DKij/BRlhWa Dj+WSAXH0Xvln/GownUCVACOY10dkFkUEpmvV3cKbSMBwGnp0aagFGKaP33O6R75 DLLzVv4/vhZQEIpUKjmwWNOYfZ5Yz5ndKJ6B3eFwYXoEQkCLiEOddP6A7Soeasss YV8jN00MUyFH9xTzvtIcsWeu5PYVcngE2vyGXkbrzWCs6vtaGQDNi7+HfzYGtKH4 jL+BHYwwxuSzn2ki1ondrtzP7+NNc6PUJfcs5/C0DwXK0ymAKlzEtxQk9infCMa2 +hCbeO1RwyqWT/pDDruVJZ2r/IcPag1rrqSPVYPz19RVxV1Td2TLlex6Nwa6JE7z p1cNpopxftrCz0Ajw4qIEJ9tP/ztZAiaf9dHHREKckMxSv41AcypVSIfpw5WTlwU somn5mbBX/r1M6F43fjEUh4NCPNBb80xu1Z1jP9AZulh1O/6Fj6jQXZVVuEOFJaq yxIy4ocLA7/1VpchZ0RpbvQpq2/I2N4H+Reqk5oodMZf0APiV3d6v3iN1YbVL0aV 8NODoCbs3IJBSaNgCjwfNyA4rtjBhup8doSJ/oTY30ZMX4uGZbpRLjJbUItK1IrD 47Hh2Ga3FhgIblMj9Fg9GqrPvU2PMplrWdbxWcpuV7klvKAx4zzDxiZiPQNqkvrX ELIqVf33GgggqmEqqNFXZDUXqSd5LIzsR7pEnaksIQ46jhQtP8WZLpkKlWTaDX2E vYQMhz9A0NT2hONOA9aLUCiyLvYjrYR9r7hhj5fpEDjOi8rIs/+NW/wrZhsJodPt WVJXx3MgHkN8tzJ40kEKBQlViQXxh2bSQjjP8WePRHX6rMmvIzWaJcOZk+lfrUGn Vd2oqQJsSntAE0KdZZSZCBTkx39xJEVS \n-----END ENCRYPTED PRIVATE KEY-----\n",
		},
		{
			name:   "private key (textInput)",
			input:  `-----BEGIN PRIVATE KEY----- MIIFJDBWBgkqhkiG9w0BBQ0wSTAxBgkqhkiG9w0BBQwwJAQQ64IduEZCqZfR5Nix HyETFQICCAAwDAYIKoZIhvcNAgkFADAUBggqhkiG9w0DBwQILyNp24M/i6MEggTI xnr7+5ltJidFtjYBhUZJz0LxmQD+2PmH4D5a22lkewL22iLcdLuQbWdvGzXSaCmP IkhQ9k/KRrzda98IkUuZ3X3uqgvqeJjXh7nqCFahfTvZhfFaINkCqzowl03wB9Dn rKxIT9C7Z00m2CSdxL+cHSYAWl1GIn/d7j99bHWa/m7H5f3BuE/xStYJG1gHrC+g aB+dzZLDygiTYqc4SaTtHDBT1PeIJqiDQOgfKGF9vZKdtrfAjPlS2MI+s2AJHO3B h6zTMjhIrQ6RX/Y5gGm5DkVeuygP2unf8EBoOc5MOkeTb9hMqnKlBz2fumo/I5mu qo5DHk4EZpnqNymRaY0glp6bjd3vMj889kiZ2Rdtf4doieYuggwYBJ4WxpQ2GQAy RfGMcDOWHIwAe9p4+prgNet6Atvtp2YEH7LjXVvmNFTGbK6Jo+Fxp5n5B/3b54kB 6RFJa6JJz4YHHEU6w8woTanr/9W2urKncbQXQW5kUW2YBSCz3ZWHNr93AY9IVXa2 csAuAcQng/8coR2A20QJybntYLHUl1p0bZMM2XgaCeqD84p4F+COo6ehgJ7w8Sp9 MlwSXiGwQlgxKpjFL0o8X2Q45yPZUaWrbUsW+i9g3ckLR/i8RD4V6gqgsi9+QRF/ XsPS9lGki6FV8UxobNzlKFmEtDN8EiqUVn3zVmBCD1Klu+/Dua9cI6N22oq/woVI kqhJN+2NCGPl72levNQkb2CsmZ1Pv6CLwrsaDaRXFEA3em736qDmdiQ5pmLq6SDo G1awzPayzUkWgrJKyAnz9SZZmYvQSPbNS3Sk/zHEbhIR/yDxn0DmoP/wuXud+YvR rdOr9bvtYYGfkpi4mWYJWsoueFV+v4991evZRAzxge/2kKn2ONyk/hMSGSWBtwNq QPWFzss2kNiE97SsZ3v8AhOAiPSdjmhh6WOudN8PPox0mx+ulI8UxuMxP3hZP2jS JomlbMvIlf1Jmuzt/I4K7lP61llvQKqsl89bhYLnQyTG7WOoyEL+VmGwyWN+XECX 8zvclFqwzNOOHCMXVfcynB4kZCWeX9L997N8ivj4SiqQ9w0YG2tFaQ6yQUnPMs27 5qBaAz7eQVuGM8Y65fs5S0m+DsJ/PvzfrPoXsKroxFhTrGqtvRkqVt7Z9FxFXctk 2OQO7lf8WgW5jGG/mqPxZ5Q4M7YoSAlkPKbcbCvcLsrVXTr+/ghwnFsGKCDCR4QR pjTqeMxFPHWVhzeSTAmZ0Kbk5i7KUug89TFxfilJa/v4bhEKDm6pcnSorWSUJlhN xxkehZ1GCJdOGNAvKwQ6Gl+2q/W0f+cMT9z2nYB/3Hx4xnqCL4EXRxq0SkoCQDVW W5uMaPy/un6jV+2qrgnt8Sn2Z9Pc0TVo/N3yXGb4mkRFRTn1IPiZePDSHsZ20oXr 4WC403Iw0gs3X5eM/s1DGdi00gU5As8vYScaMQXb68Th+fFPfvc9o9lVfeHkiM/L KcbdeGWnYuc8lj1AVSEubiHLoFGvePk8jsDaWE8oaV4IhLTsU5x3Baj5rNUfqMZv MhFsjpTpmdbgroekHpP1hqeXp8/0TJ/1o96ublAqC5LJpfbYOVGeP3OJgx/ASmrp RA3rnyTeLSAN2KOdvaSyZxGmCxYMLu9p -----END PRIVATE KEY-----`,
			output: "-----BEGIN PRIVATE KEY-----\n MIIFJDBWBgkqhkiG9w0BBQ0wSTAxBgkqhkiG9w0BBQwwJAQQ64IduEZCqZfR5Nix HyETFQICCAAwDAYIKoZIhvcNAgkFADAUBggqhkiG9w0DBwQILyNp24M/i6MEggTI xnr7+5ltJidFtjYBhUZJz0LxmQD+2PmH4D5a22lkewL22iLcdLuQbWdvGzXSaCmP IkhQ9k/KRrzda98IkUuZ3X3uqgvqeJjXh7nqCFahfTvZhfFaINkCqzowl03wB9Dn rKxIT9C7Z00m2CSdxL+cHSYAWl1GIn/d7j99bHWa/m7H5f3BuE/xStYJG1gHrC+g aB+dzZLDygiTYqc4SaTtHDBT1PeIJqiDQOgfKGF9vZKdtrfAjPlS2MI+s2AJHO3B h6zTMjhIrQ6RX/Y5gGm5DkVeuygP2unf8EBoOc5MOkeTb9hMqnKlBz2fumo/I5mu qo5DHk4EZpnqNymRaY0glp6bjd3vMj889kiZ2Rdtf4doieYuggwYBJ4WxpQ2GQAy RfGMcDOWHIwAe9p4+prgNet6Atvtp2YEH7LjXVvmNFTGbK6Jo+Fxp5n5B/3b54kB 6RFJa6JJz4YHHEU6w8woTanr/9W2urKncbQXQW5kUW2YBSCz3ZWHNr93AY9IVXa2 csAuAcQng/8coR2A20QJybntYLHUl1p0bZMM2XgaCeqD84p4F+COo6ehgJ7w8Sp9 MlwSXiGwQlgxKpjFL0o8X2Q45yPZUaWrbUsW+i9g3ckLR/i8RD4V6gqgsi9+QRF/ XsPS9lGki6FV8UxobNzlKFmEtDN8EiqUVn3zVmBCD1Klu+/Dua9cI6N22oq/woVI kqhJN+2NCGPl72levNQkb2CsmZ1Pv6CLwrsaDaRXFEA3em736qDmdiQ5pmLq6SDo G1awzPayzUkWgrJKyAnz9SZZmYvQSPbNS3Sk/zHEbhIR/yDxn0DmoP/wuXud+YvR rdOr9bvtYYGfkpi4mWYJWsoueFV+v4991evZRAzxge/2kKn2ONyk/hMSGSWBtwNq QPWFzss2kNiE97SsZ3v8AhOAiPSdjmhh6WOudN8PPox0mx+ulI8UxuMxP3hZP2jS JomlbMvIlf1Jmuzt/I4K7lP61llvQKqsl89bhYLnQyTG7WOoyEL+VmGwyWN+XECX 8zvclFqwzNOOHCMXVfcynB4kZCWeX9L997N8ivj4SiqQ9w0YG2tFaQ6yQUnPMs27 5qBaAz7eQVuGM8Y65fs5S0m+DsJ/PvzfrPoXsKroxFhTrGqtvRkqVt7Z9FxFXctk 2OQO7lf8WgW5jGG/mqPxZ5Q4M7YoSAlkPKbcbCvcLsrVXTr+/ghwnFsGKCDCR4QR pjTqeMxFPHWVhzeSTAmZ0Kbk5i7KUug89TFxfilJa/v4bhEKDm6pcnSorWSUJlhN xxkehZ1GCJdOGNAvKwQ6Gl+2q/W0f+cMT9z2nYB/3Hx4xnqCL4EXRxq0SkoCQDVW W5uMaPy/un6jV+2qrgnt8Sn2Z9Pc0TVo/N3yXGb4mkRFRTn1IPiZePDSHsZ20oXr 4WC403Iw0gs3X5eM/s1DGdi00gU5As8vYScaMQXb68Th+fFPfvc9o9lVfeHkiM/L KcbdeGWnYuc8lj1AVSEubiHLoFGvePk8jsDaWE8oaV4IhLTsU5x3Baj5rNUfqMZv MhFsjpTpmdbgroekHpP1hqeXp8/0TJ/1o96ublAqC5LJpfbYOVGeP3OJgx/ASmrp RA3rnyTeLSAN2KOdvaSyZxGmCxYMLu9p \n-----END PRIVATE KEY-----\n",
		},
		{
			name:   "private key (textArea)",
			input:  "-----BEGIN PRIVATE KEY-----\nMIIFJDBWBgkqhkiG9w0BBQ0wSTAxBgkqhkiG9w0BBQwwJAQQpWee/aYJAeHHT9AS\npIo+jQICCAAwDAYIKoZIhvcNAgkFADAUBggqhkiG9w0DBwQInSbkcxgNEisEggTI\n/3+KEvhVubn3GXS/w0QvJz0qR/gjgWSZ5e+c8U5DmVAyjzftS/QNFIX8ArYDwFUh\nCy9wJEmbPRlcRloXTBsk5IMT0MYIa/4zGxfqPWfgdkxrJzS2sCQP+FwsgkSUEvYj\nI7UEJ8kxmfew30RCJRJlNdzYPg8HAYlVizyemWxhrnFT8HE4Len+ILJUN0HGfra6\nU8pLI6MKnGRqLZBWIhc+2JJ/UqWQexVClN/gNV3xkC5CM7CsRsDRJw7bbWFwH2Eo\n0VStFV3DVpjf++VnPoRlRi++3olXVxO1I2e+SR1fU0CVjzXE+Q+ltWJHiBsQ7kWt\nM7weOfvd1AxAYhM7HzHOyI5JyawaBUnc2PNqzrDv1AOU8HIOe1JCuvj4RWI//BpE\nsZmjjGBMRzTsorWMILaWFEnC5lefjd06Cmag5jsLoLrZeewqwix7+r8SYVptnl5O\njNO9lZU83HJwH5W9TPHB7OCQPOMGqjAnIeDEwLPjBWGdylyf/BZamvoONG74f5kq\n8I3bX4rxMM71vg6xWcS+MKKn/4ch3oIjuN+lUOVJH1G2wEROVzbQknWns6JM+Jsa\nfQjQh0YigdVwbHC99wCLtwVVXcmpA5Jj2z8wvqCYnb4MlaYt8Ld7hVaaF13tp9lM\nMazxIOIJmHuX4BB3wVFTCygHqKzEUczyMnjKfAKO1BL72ZYQAI9nbZmSUuXuvxun\nKPk6XqTxSJCjGHQxIFkEJVHT9qhxI5MUgdo6R+BVvPCxdo7Xnikw3DKij/BRlhWa\nDj+WSAXH0Xvln/GownUCVACOY10dkFkUEpmvV3cKbSMBwGnp0aagFGKaP33O6R75\nDLLzVv4/vhZQEIpUKjmwWNOYfZ5Yz5ndKJ6B3eFwYXoEQkCLiEOddP6A7Soeasss\nYV8jN00MUyFH9xTzvtIcsWeu5PYVcngE2vyGXkbrzWCs6vtaGQDNi7+HfzYGtKH4\njL+BHYwwxuSzn2ki1ondrtzP7+NNc6PUJfcs5/C0DwXK0ymAKlzEtxQk9infCMa2\n+hCbeO1RwyqWT/pDDruVJZ2r/IcPag1rrqSPVYPz19RVxV1Td2TLlex6Nwa6JE7z\np1cNpopxftrCz0Ajw4qIEJ9tP/ztZAiaf9dHHREKckMxSv41AcypVSIfpw5WTlwU\nsomn5mbBX/r1M6F43fjEUh4NCPNBb80xu1Z1jP9AZulh1O/6Fj6jQXZVVuEOFJaq\nyxIy4ocLA7/1VpchZ0RpbvQpq2/I2N4H+Reqk5oodMZf0APiV3d6v3iN1YbVL0aV\n8NODoCbs3IJBSaNgCjwfNyA4rtjBhup8doSJ/oTY30ZMX4uGZbpRLjJbUItK1IrD\n47Hh2Ga3FhgIblMj9Fg9GqrPvU2PMplrWdbxWcpuV7klvKAx4zzDxiZiPQNqkvrX\nELIqVf33GgggqmEqqNFXZDUXqSd5LIzsR7pEnaksIQ46jhQtP8WZLpkKlWTaDX2E\nvYQMhz9A0NT2hONOA9aLUCiyLvYjrYR9r7hhj5fpEDjOi8rIs/+NW/wrZhsJodPt\nWVJXx3MgHkN8tzJ40kEKBQlViQXxh2bSQjjP8WePRHX6rMmvIzWaJcOZk+lfrUGn\nVd2oqQJsSntAE0KdZZSZCBTkx39xJEVS\n-----END PRIVATE KEY-----",
			output: "-----BEGIN PRIVATE KEY-----\n MIIFJDBWBgkqhkiG9w0BBQ0wSTAxBgkqhkiG9w0BBQwwJAQQpWee/aYJAeHHT9AS pIo+jQICCAAwDAYIKoZIhvcNAgkFADAUBggqhkiG9w0DBwQInSbkcxgNEisEggTI /3+KEvhVubn3GXS/w0QvJz0qR/gjgWSZ5e+c8U5DmVAyjzftS/QNFIX8ArYDwFUh Cy9wJEmbPRlcRloXTBsk5IMT0MYIa/4zGxfqPWfgdkxrJzS2sCQP+FwsgkSUEvYj I7UEJ8kxmfew30RCJRJlNdzYPg8HAYlVizyemWxhrnFT8HE4Len+ILJUN0HGfra6 U8pLI6MKnGRqLZBWIhc+2JJ/UqWQexVClN/gNV3xkC5CM7CsRsDRJw7bbWFwH2Eo 0VStFV3DVpjf++VnPoRlRi++3olXVxO1I2e+SR1fU0CVjzXE+Q+ltWJHiBsQ7kWt M7weOfvd1AxAYhM7HzHOyI5JyawaBUnc2PNqzrDv1AOU8HIOe1JCuvj4RWI//BpE sZmjjGBMRzTsorWMILaWFEnC5lefjd06Cmag5jsLoLrZeewqwix7+r8SYVptnl5O jNO9lZU83HJwH5W9TPHB7OCQPOMGqjAnIeDEwLPjBWGdylyf/BZamvoONG74f5kq 8I3bX4rxMM71vg6xWcS+MKKn/4ch3oIjuN+lUOVJH1G2wEROVzbQknWns6JM+Jsa fQjQh0YigdVwbHC99wCLtwVVXcmpA5Jj2z8wvqCYnb4MlaYt8Ld7hVaaF13tp9lM MazxIOIJmHuX4BB3wVFTCygHqKzEUczyMnjKfAKO1BL72ZYQAI9nbZmSUuXuvxun KPk6XqTxSJCjGHQxIFkEJVHT9qhxI5MUgdo6R+BVvPCxdo7Xnikw3DKij/BRlhWa Dj+WSAXH0Xvln/GownUCVACOY10dkFkUEpmvV3cKbSMBwGnp0aagFGKaP33O6R75 DLLzVv4/vhZQEIpUKjmwWNOYfZ5Yz5ndKJ6B3eFwYXoEQkCLiEOddP6A7Soeasss YV8jN00MUyFH9xTzvtIcsWeu5PYVcngE2vyGXkbrzWCs6vtaGQDNi7+HfzYGtKH4 jL+BHYwwxuSzn2ki1ondrtzP7+NNc6PUJfcs5/C0DwXK0ymAKlzEtxQk9infCMa2 +hCbeO1RwyqWT/pDDruVJZ2r/IcPag1rrqSPVYPz19RVxV1Td2TLlex6Nwa6JE7z p1cNpopxftrCz0Ajw4qIEJ9tP/ztZAiaf9dHHREKckMxSv41AcypVSIfpw5WTlwU somn5mbBX/r1M6F43fjEUh4NCPNBb80xu1Z1jP9AZulh1O/6Fj6jQXZVVuEOFJaq yxIy4ocLA7/1VpchZ0RpbvQpq2/I2N4H+Reqk5oodMZf0APiV3d6v3iN1YbVL0aV 8NODoCbs3IJBSaNgCjwfNyA4rtjBhup8doSJ/oTY30ZMX4uGZbpRLjJbUItK1IrD 47Hh2Ga3FhgIblMj9Fg9GqrPvU2PMplrWdbxWcpuV7klvKAx4zzDxiZiPQNqkvrX ELIqVf33GgggqmEqqNFXZDUXqSd5LIzsR7pEnaksIQ46jhQtP8WZLpkKlWTaDX2E vYQMhz9A0NT2hONOA9aLUCiyLvYjrYR9r7hhj5fpEDjOi8rIs/+NW/wrZhsJodPt WVJXx3MgHkN8tzJ40kEKBQlViQXxh2bSQjjP8WePRHX6rMmvIzWaJcOZk+lfrUGn Vd2oqQJsSntAE0KdZZSZCBTkx39xJEVS \n-----END PRIVATE KEY-----\n",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.output, FormatPemContent(tc.input))
		})
	}
}

func TestDestinationConfigKeys(t *testing.T) {
	for _, whType := range WarehouseDestinations {
		t.Run(whType, func(t *testing.T) {
			require.Contains(t, WHDestNameMap, whType)

			whName := WHDestNameMap[whType]
			configKey := fmt.Sprintf("Warehouse.%s.feature", whName)
			got := config.ConfigKeyToEnv(config.DefaultEnvPrefix, configKey)
			expected := fmt.Sprintf("RSERVER_WAREHOUSE_%s_FEATURE", strings.ToUpper(whName))
			require.Equal(t, got, expected)
		})
	}
}

func TestGetS3Location(t *testing.T) {
	inputs := []struct {
		location   string
		s3Location string
		region     string
	}{
		{
			location:   "https://test-bucket.s3.amazonaws.com/test-object.csv",
			s3Location: "s3://test-bucket/test-object.csv",
			region:     "",
		},
		{
			location:   "https://test-bucket.s3.any-region.amazonaws.com/test-object.csv",
			s3Location: "s3://test-bucket/test-object.csv",
			region:     "any-region",
		},
		{
			location:   "https://my.test-bucket.s3.amazonaws.com/test-object.csv",
			s3Location: "s3://my.test-bucket/test-object.csv",
			region:     "",
		},
		{
			location:   "https://my.test-bucket.s3.us-west-1.amazonaws.com/test-object.csv",
			s3Location: "s3://my.test-bucket/test-object.csv",
			region:     "us-west-1",
		},
		{
			location:   "https://my.example.s3.bucket.s3.us-west-1.amazonaws.com/test-object.csv",
			s3Location: "s3://my.example.s3.bucket/test-object.csv",
			region:     "us-west-1",
		},
		{
			location:   "https://s3.amazonaws.com/test-bucket/test-object.csv",
			s3Location: "s3://test-bucket/test-object.csv",
			region:     "",
		},
		{
			location:   "https://s3.any-region.amazonaws.com/test-bucket/test-object.csv",
			s3Location: "s3://test-bucket/test-object.csv",
			region:     "any-region",
		},
		{
			location:   "https://s3.amazonaws.com/my.test-bucket/test-object.csv",
			s3Location: "s3://my.test-bucket/test-object.csv",
			region:     "",
		},
		{
			location:   "https://s3.us-west-1.amazonaws.com/my.test-bucket/test-object.csv",
			s3Location: "s3://my.test-bucket/test-object.csv",
			region:     "us-west-1",
		},
		{
			location:   "https://s3.amazonaws.com/bucket.with.a.dot/test-object.csv",
			s3Location: "s3://bucket.with.a.dot/test-object.csv",
			region:     "",
		},
		{
			location:   "https://s3.amazonaws.com/s3.rudderstack/test-object.csv",
			s3Location: "s3://s3.rudderstack/test-object.csv",
			region:     "",
		},
		{
			location:   "https://google.com",
			s3Location: "",
			region:     "",
		},
	}

	for _, input := range inputs {
		s3Location, region := GetS3Location(input.location)
		require.Equal(t, s3Location, input.s3Location)
		require.Equal(t, region, input.region)
	}
}

func TestCaptureRegexGroup(t *testing.T) {
	t.Run("Matches", func(t *testing.T) {
		inputs := []struct {
			regex   *regexp.Regexp
			pattern string
			groups  map[string]string
		}{
			{
				regex:   S3PathStyleRegex,
				pattern: "https://s3.amazonaws.com/bucket.with.a.dot/keyname",
				groups: map[string]string{
					"bucket":  "bucket.with.a.dot",
					"keyname": "keyname",
					"region":  "",
				},
			},
			{
				regex:   S3PathStyleRegex,
				pattern: "https://s3.us-east.amazonaws.com/bucket.with.a.dot/keyname",
				groups: map[string]string{
					"bucket":  "bucket.with.a.dot",
					"keyname": "keyname",
					"region":  "us-east",
				},
			},
			{
				regex:   S3VirtualHostedRegex,
				pattern: "https://bucket.with.a.dot.s3.amazonaws.com/keyname",
				groups: map[string]string{
					"bucket":  "bucket.with.a.dot",
					"keyname": "keyname",
					"region":  "",
				},
			},
			{
				regex:   S3VirtualHostedRegex,
				pattern: "https://bucket.with.a.dot.s3.amazonaws.com/keyname",
				groups: map[string]string{
					"bucket":  "bucket.with.a.dot",
					"keyname": "keyname",
					"region":  "",
				},
			},
		}
		for _, input := range inputs {
			got, err := CaptureRegexGroup(input.regex, input.pattern)
			require.NoError(t, err)
			require.Equal(t, got, input.groups)
		}
	})
	t.Run("Not Matches", func(t *testing.T) {
		inputs := []struct {
			regex   *regexp.Regexp
			pattern string
		}{
			{
				regex:   S3PathStyleRegex,
				pattern: "https://google.com",
			},
			{
				regex:   S3VirtualHostedRegex,
				pattern: "https://google.com",
			},
		}
		for _, input := range inputs {
			_, err := CaptureRegexGroup(input.regex, input.pattern)
			require.Error(t, err)
		}
	})
}

func TestGetS3LocationFolder(t *testing.T) {
	inputs := []struct {
		s3Location       string
		s3LocationFolder string
	}{
		{
			s3Location:       "https://test-bucket.s3.amazonaws.com/myfolder/test-object.csv",
			s3LocationFolder: "s3://test-bucket/myfolder",
		},
		{
			s3Location:       "https://test-bucket.s3.eu-west-2.amazonaws.com/myfolder/test-object.csv",
			s3LocationFolder: "s3://test-bucket/myfolder",
		},
		{
			s3Location:       "https://my.test-bucket.s3.eu-west-2.amazonaws.com/myfolder/test-object.csv",
			s3LocationFolder: "s3://my.test-bucket/myfolder",
		},
	}
	for _, input := range inputs {
		s3LocationFolder := GetS3LocationFolder(input.s3Location)
		require.Equal(t, s3LocationFolder, input.s3LocationFolder)
	}
}

func TestGetS3Locations(t *testing.T) {
	inputs := []LoadFile{
		{Location: "https://test-bucket.s3.amazonaws.com/test-object.csv"},
		{Location: "https://test-bucket.s3.eu-west-1.amazonaws.com/test-object.csv"},
		{Location: "https://my.test-bucket.s3.amazonaws.com/test-object.csv"},
		{Location: "https://my.test-bucket.s3.us-west-1.amazonaws.com/test-object.csv"},
	}
	outputs := []LoadFile{
		{Location: "s3://test-bucket/test-object.csv"},
		{Location: "s3://test-bucket/test-object.csv"},
		{Location: "s3://my.test-bucket/test-object.csv"},
		{Location: "s3://my.test-bucket/test-object.csv"},
	}

	s3Locations := GetS3Locations(inputs)
	require.Equal(t, s3Locations, outputs)
}

func TestGetGCSLocation(t *testing.T) {
	inputs := []struct {
		location    string
		gcsLocation string
		format      string
	}{
		{
			location:    "https://storage.googleapis.com/test-bucket/test-object.csv",
			gcsLocation: "gs://test-bucket/test-object.csv",
		},
		{
			location:    "https://storage.googleapis.com/my.test-bucket/test-object.csv",
			gcsLocation: "gs://my.test-bucket/test-object.csv",
		},
		{
			location:    "https://storage.googleapis.com/test-bucket/test-object.csv",
			gcsLocation: "gcs://test-bucket/test-object.csv",
			format:      "gcs",
		},
		{
			location:    "https://storage.googleapis.com/my.test-bucket/test-object.csv",
			gcsLocation: "gcs://my.test-bucket/test-object.csv",
			format:      "gcs",
		},
	}
	for _, input := range inputs {
		gcsLocation := GetGCSLocation(input.location, GCSLocationOptions{
			TLDFormat: input.format,
		})
		require.Equal(t, gcsLocation, input.gcsLocation)
	}
}

func TestGetGCSLocationFolder(t *testing.T) {
	inputs := []struct {
		location          string
		gcsLocationFolder string
	}{
		{
			location:          "https://storage.googleapis.com/test-bucket/test-object.csv",
			gcsLocationFolder: "gs://test-bucket",
		},
		{
			location:          "https://storage.googleapis.com/my.test-bucket/test-object.csv",
			gcsLocationFolder: "gs://my.test-bucket",
		},
	}
	for _, input := range inputs {
		gcsLocationFolder := GetGCSLocationFolder(input.location, GCSLocationOptions{})
		require.Equal(t, gcsLocationFolder, input.gcsLocationFolder)
	}
}

func TestGetGCSLocations(t *testing.T) {
	inputs := []LoadFile{
		{Location: "https://storage.googleapis.com/test-bucket/test-object.csv"},
		{Location: "https://storage.googleapis.com/my.test-bucket/test-object.csv"},
		{Location: "https://storage.googleapis.com/my.test-bucket2/test-object.csv"},
		{Location: "https://storage.googleapis.com/my.test-bucket/test-object2.csv"},
	}
	outputs := []string{
		"gs://test-bucket/test-object.csv",
		"gs://my.test-bucket/test-object.csv",
		"gs://my.test-bucket2/test-object.csv",
		"gs://my.test-bucket/test-object2.csv",
	}

	gcsLocations := GetGCSLocations(inputs, GCSLocationOptions{})
	require.Equal(t, gcsLocations, outputs)
}

func TestGetAzureBlobLocation(t *testing.T) {
	inputs := []struct {
		location       string
		azBlobLocation string
	}{
		{
			location:       "https://myproject.blob.core.windows.net/test-bucket/test-object.csv",
			azBlobLocation: "azure://myproject.blob.core.windows.net/test-bucket/test-object.csv",
		},
	}
	for _, input := range inputs {
		azBlobLocation := GetAzureBlobLocation(input.location)
		require.Equal(t, azBlobLocation, input.azBlobLocation)
	}
}

func TestGetAzureBlobLocationFolder(t *testing.T) {
	inputs := []struct {
		location             string
		azBlobLocationFolder string
	}{
		{
			location:             "https://myproject.blob.core.windows.net/test-bucket/myfolder/test-object.csv",
			azBlobLocationFolder: "azure://myproject.blob.core.windows.net/test-bucket/myfolder",
		},
	}
	for _, input := range inputs {
		azBlobLocationFolder := GetAzureBlobLocationFolder(input.location)
		require.Equal(t, azBlobLocationFolder, input.azBlobLocationFolder)
	}
}

func TestToSafeNamespace(t *testing.T) {
	config.Set("Warehouse.bigquery.skipNamespaceSnakeCasing", true)
	defer config.Reset()

	inputs := []struct {
		provider      string
		namespace     string
		safeNamespace string
	}{
		{
			namespace:     "omega",
			safeNamespace: "omega",
		},
		{
			namespace:     "omega v2 ",
			safeNamespace: "omega_v_2",
		},
		{
			namespace:     "9mega",
			safeNamespace: "_9_mega",
		},
		{
			namespace:     "mega&",
			safeNamespace: "mega",
		},
		{
			namespace:     "ome$ga",
			safeNamespace: "ome_ga",
		},
		{
			namespace:     "omega$",
			safeNamespace: "omega",
		},
		{
			namespace:     "ome_ ga",
			safeNamespace: "ome_ga",
		},
		{
			namespace:     "9mega________-________90",
			safeNamespace: "_9_mega_90",
		},
		{
			namespace:     "Cízǔ",
			safeNamespace: "c_z",
		},
		{
			namespace:     "Rudderstack",
			safeNamespace: "rudderstack",
		},
		{
			namespace:     "___",
			safeNamespace: "stringempty",
		},
		{
			provider:      "RS",
			namespace:     "group",
			safeNamespace: "_group",
		},
		{
			provider:      "RS",
			namespace:     "k3_namespace",
			safeNamespace: "k_3_namespace",
		},
		{
			provider:      "BQ",
			namespace:     "k3_namespace",
			safeNamespace: "k3_namespace",
		},
	}
	for _, input := range inputs {
		safeNamespace := ToSafeNamespace(input.provider, input.namespace)
		require.Equal(t, safeNamespace, input.safeNamespace)
	}
}

func TestGetObjectLocation(t *testing.T) {
	inputs := []struct {
		provider       string
		location       string
		objectLocation string
	}{
		{
			provider:       "S3",
			location:       "https://test-bucket.s3.amazonaws.com/test-object.csv",
			objectLocation: "s3://test-bucket/test-object.csv",
		},
		{
			provider:       "GCS",
			location:       "https://storage.googleapis.com/my.test-bucket/test-object.csv",
			objectLocation: "gcs://my.test-bucket/test-object.csv",
		},
		{
			provider:       "AZURE_BLOB",
			location:       "https://myproject.blob.core.windows.net/test-bucket/test-object.csv",
			objectLocation: "azure://myproject.blob.core.windows.net/test-bucket/test-object.csv",
		},
	}

	for _, input := range inputs {
		t.Run(input.provider, func(t *testing.T) {
			objectLocation := GetObjectLocation(input.provider, input.location)
			require.Equal(t, objectLocation, input.objectLocation)
		})
	}
}

func TestGetObjectFolder(t *testing.T) {
	inputs := []struct {
		provider     string
		location     string
		objectFolder string
	}{
		{
			provider:     "S3",
			location:     "https://test-bucket.s3.amazonaws.com/myfolder/test-object.csv",
			objectFolder: "s3://test-bucket/myfolder",
		},
		{
			provider:     "GCS",
			location:     "https://storage.googleapis.com/test-bucket/test-object.csv",
			objectFolder: "gcs://test-bucket",
		},
		{
			provider:     "AZURE_BLOB",
			location:     "https://myproject.blob.core.windows.net/test-bucket/myfolder/test-object.csv",
			objectFolder: "azure://myproject.blob.core.windows.net/test-bucket/myfolder",
		},
	}

	for _, input := range inputs {
		t.Run(input.provider, func(t *testing.T) {
			objectFolder := GetObjectFolder(input.provider, input.location)
			require.Equal(t, objectFolder, input.objectFolder)
		})
	}
}

func TestGetObjectFolderForDeltalake(t *testing.T) {
	inputs := []struct {
		provider     string
		location     string
		objectFolder string
	}{
		{
			provider:     "S3",
			location:     "https://test-bucket.s3.amazonaws.com/myfolder/test-object.csv",
			objectFolder: "s3://test-bucket/myfolder",
		},
		{
			provider:     "GCS",
			location:     "https://storage.googleapis.com/test-bucket/test-object.csv",
			objectFolder: "gs://test-bucket",
		},
		{
			provider:     "AZURE_BLOB",
			location:     "https://myproject.blob.core.windows.net/test-bucket/myfolder/test-object.csv",
			objectFolder: "wasbs://test-bucket@myproject.blob.core.windows.net/myfolder",
		},
	}

	for _, input := range inputs {
		t.Run(input.provider, func(t *testing.T) {
			objectFolder := GetObjectFolderForDeltalake(input.provider, input.location)
			require.Equal(t, objectFolder, input.objectFolder)
		})
	}
}

func TestDoubleQuoteAndJoinByComma(t *testing.T) {
	names := []string{"Samantha Edwards", "Samantha Smith", "Holly Miller", "Tammie Tyler", "Gina Richards"}
	want := "\"Samantha Edwards\",\"Samantha Smith\",\"Holly Miller\",\"Tammie Tyler\",\"Gina Richards\""
	got := DoubleQuoteAndJoinByComma(names)
	require.Equal(t, got, want)
}

func TestSortColumnKeysFromColumnMap(t *testing.T) {
	columnMap := model.TableSchema{"k5": "V5", "k4": "V4", "k3": "V3", "k2": "V2", "k1": "V1"}
	want := []string{"k1", "k2", "k3", "k4", "k5"}
	got := SortColumnKeysFromColumnMap(columnMap)
	require.Equal(t, got, want)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v want %#v input %#v", got, want, columnMap)
	}
}

func TestTimingFromJSONString(t *testing.T) {
	inputs := []struct {
		timingsMap        sql.NullString
		loadFilesEpochStr string
		status            string
	}{
		{
			timingsMap: sql.NullString{
				String: "{\"generating_upload_schema\":\"2022-07-04T16:09:04.000Z\"}",
			},
			status:            "generating_upload_schema",
			loadFilesEpochStr: "2022-07-04T16:09:04.000Z",
		},
		{
			timingsMap: sql.NullString{
				String: "{}",
			},
			status:            "",
			loadFilesEpochStr: "0001-01-01T00:00:00.000Z",
		},
	}
	for _, input := range inputs {
		loadFilesEpochTime, err := time.Parse(misc.RFC3339Milli, input.loadFilesEpochStr)
		require.NoError(t, err)

		status, recordedTime := TimingFromJSONString(input.timingsMap)
		require.Equal(t, status, input.status)
		require.Equal(t, loadFilesEpochTime, recordedTime)
	}
}

func TestJoinWithFormatting(t *testing.T) {
	separator := ","
	format := func(idx int, name string) string {
		return fmt.Sprintf(`%s_v%d`, name, idx+1)
	}
	inputs := []struct {
		keys  []string
		value string
	}{
		{
			keys:  []string{"k1", "k2"},
			value: "k1_v1,k2_v2",
		},
		{
			keys: []string{},
		},
	}
	for _, input := range inputs {
		value := JoinWithFormatting(input.keys, format, separator)
		require.Equal(t, value, input.value)
	}
}

func TestToProviderCase(t *testing.T) {
	inputs := []struct {
		provider string
		value    string
	}{
		{
			provider: SNOWFLAKE,
			value:    "RAND",
		},
		{
			provider: POSTGRES,
			value:    "rand",
		},
		{
			provider: CLICKHOUSE,
			value:    "rand",
		},
	}
	for _, input := range inputs {
		t.Run(input.provider, func(t *testing.T) {
			value := ToProviderCase(input.provider, "rand")
			require.Equal(t, value, input.value)
		})
	}
}

func TestSnowflakeCloudProvider(t *testing.T) {
	inputs := []struct {
		config   interface{}
		provider string
	}{
		{
			config: map[string]interface{}{
				"cloudProvider": "GCP",
			},
			provider: "GCP",
		},
		{
			config:   map[string]interface{}{},
			provider: "AWS",
		},
	}
	for _, input := range inputs {
		provider := SnowflakeCloudProvider(input.config)
		require.Equal(t, provider, input.provider)
	}
}

func TestObjectStorageType(t *testing.T) {
	inputs := []struct {
		destType         string
		config           interface{}
		useRudderStorage bool
		storageType      string
	}{
		{
			config:           map[string]interface{}{},
			useRudderStorage: true,
			storageType:      "S3",
		},
		{
			destType:    "RS",
			config:      map[string]interface{}{},
			storageType: "S3",
		},
		{
			destType:    "S3_DATALAKE",
			config:      map[string]interface{}{},
			storageType: "S3",
		},
		{
			destType:    "BQ",
			config:      map[string]interface{}{},
			storageType: "GCS",
		},
		{
			destType:    "GCS_DATALAKE",
			config:      map[string]interface{}{},
			storageType: "GCS",
		},
		{
			destType:    "AZURE_DATALAKE",
			config:      map[string]interface{}{},
			storageType: "AZURE_BLOB",
		},
		{
			destType:    "SNOWFLAKE",
			config:      map[string]interface{}{},
			storageType: "S3",
		},
		{
			destType: "SNOWFLAKE",
			config: map[string]interface{}{
				"cloudProvider": "AZURE",
			},
			storageType: "AZURE_BLOB",
		},
		{
			destType: "SNOWFLAKE",
			config: map[string]interface{}{
				"cloudProvider": "GCP",
			},
			storageType: "GCS",
		},
		{
			destType: "POSTGRES",
			config: map[string]interface{}{
				"bucketProvider": "GCP",
			},
			storageType: "GCP",
		},
		{
			destType: "POSTGRES",
			config:   map[string]interface{}{},
		},
	}
	for _, input := range inputs {
		provider := ObjectStorageType(input.destType, input.config, input.useRudderStorage)
		require.Equal(t, provider, input.storageType)
	}
}

func TestGetTablePathInObjectStorage(t *testing.T) {
	t.Setenv("WAREHOUSE_DATALAKE_FOLDER_NAME", "rudder-test-payload")
	inputs := []struct {
		namespace string
		tableName string
		expected  string
	}{
		{
			namespace: "rudderstack_setup_test",
			tableName: "setup_test_staging",
			expected:  "rudder-test-payload/rudderstack_setup_test/setup_test_staging",
		},
	}
	for _, input := range inputs {
		got := GetTablePathInObjectStorage(input.namespace, input.tableName)
		require.Equal(t, got, input.expected)
	}
}

func TestGetTempFileExtension(t *testing.T) {
	inputs := []struct {
		destType string
		expected string
	}{
		{
			destType: BQ,
			expected: "json.gz",
		},
		{
			destType: RS,
			expected: "csv.gz",
		},
		{
			destType: SNOWFLAKE,
			expected: "csv.gz",
		},
		{
			destType: POSTGRES,
			expected: "csv.gz",
		},
		{
			destType: CLICKHOUSE,
			expected: "csv.gz",
		},
		{
			destType: MSSQL,
			expected: "csv.gz",
		},
		{
			destType: AzureSynapse,
			expected: "csv.gz",
		},
		{
			destType: DELTALAKE,
			expected: "csv.gz",
		},
		{
			destType: S3Datalake,
			expected: "csv.gz",
		},
		{
			destType: GCSDatalake,
			expected: "csv.gz",
		},
		{
			destType: AzureDatalake,
			expected: "csv.gz",
		},
	}
	for _, input := range inputs {
		got := GetTempFileExtension(input.destType)
		require.Equal(t, got, input.expected)
	}
}

func TestWarehouseT_GetBoolDestinationConfig(t *testing.T) {
	inputs := []struct {
		warehouse model.Warehouse
		expected  bool
	}{
		{
			warehouse: model.Warehouse{
				Destination: backendconfig.DestinationT{},
			},
			expected: false,
		},
		{
			warehouse: model.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{},
				},
			},
			expected: false,
		},
		{
			warehouse: model.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{
						"useRudderStorage": "true",
					},
				},
			},
			expected: false,
		},
		{
			warehouse: model.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{
						"useRudderStorage": false,
					},
				},
			},
			expected: false,
		},
		{
			warehouse: model.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{
						"useRudderStorage": true,
					},
				},
			},
			expected: true,
		},
	}
	for idx, input := range inputs {
		got := input.warehouse.GetBoolDestinationConfig(model.UseRudderStorageSetting)
		want := input.expected
		if got != want {
			t.Errorf("got %t expected %t input %d", got, want, idx)
		}
	}
}

func TestGetLoadFileFormat(t *testing.T) {
	inputs := []struct {
		whType   string
		expected string
	}{
		{
			whType:   BQ,
			expected: "json.gz",
		},
		{
			whType:   RS,
			expected: "csv.gz",
		},
		{
			whType:   SNOWFLAKE,
			expected: "csv.gz",
		},
		{
			whType:   POSTGRES,
			expected: "csv.gz",
		},
		{
			whType:   CLICKHOUSE,
			expected: "csv.gz",
		},
		{
			whType:   MSSQL,
			expected: "csv.gz",
		},
		{
			whType:   AzureSynapse,
			expected: "csv.gz",
		},
		{
			whType:   DELTALAKE,
			expected: "csv.gz",
		},
		{
			whType:   S3Datalake,
			expected: "parquet",
		},
		{
			whType:   GCSDatalake,
			expected: "parquet",
		},
		{
			whType:   AzureDatalake,
			expected: "parquet",
		},
	}
	for _, input := range inputs {
		got := GetLoadFileFormat(GetLoadFileType(input.whType))
		require.Equal(t, got, input.expected)
	}
}

func TestGetLoadFileType(t *testing.T) {
	inputs := []struct {
		whType   string
		expected string
	}{
		{
			whType:   BQ,
			expected: "json",
		},
		{
			whType:   RS,
			expected: "csv",
		},
		{
			whType:   SNOWFLAKE,
			expected: "csv",
		},
		{
			whType:   POSTGRES,
			expected: "csv",
		},
		{
			whType:   CLICKHOUSE,
			expected: "csv",
		},
		{
			whType:   MSSQL,
			expected: "csv",
		},
		{
			whType:   AzureSynapse,
			expected: "csv",
		},
		{
			whType:   DELTALAKE,
			expected: "csv",
		},
		{
			whType:   S3Datalake,
			expected: "parquet",
		},
		{
			whType:   GCSDatalake,
			expected: "parquet",
		},
		{
			whType:   AzureDatalake,
			expected: "parquet",
		},
	}
	for _, input := range inputs {
		got := GetLoadFileType(input.whType)
		require.Equal(t, got, input.expected)
	}
}

func TestGetTimeWindow(t *testing.T) {
	inputs := []struct {
		ts       time.Time
		expected time.Time
	}{
		{
			ts:       time.Date(2020, 4, 27, 20, 23, 54, 3424534, time.UTC),
			expected: time.Date(2020, 4, 27, 20, 0, 0, 0, time.UTC),
		},
	}
	for _, input := range inputs {
		got := GetTimeWindow(input.ts)
		require.Equal(t, got, input.expected)
	}
}

func TestGetWarehouseIdentifier(t *testing.T) {
	inputs := []struct {
		destType      string
		sourceID      string
		destinationID string
		expected      string
	}{
		{
			destType:      "RS",
			sourceID:      "sourceID",
			destinationID: "destinationID",
			expected:      "RS:sourceID:destinationID",
		},
	}
	for _, input := range inputs {
		got := GetWarehouseIdentifier(input.destType, input.sourceID, input.destinationID)
		require.Equal(t, got, input.expected)
	}
}

func TestCreateAWSSessionConfig(t *testing.T) {
	rudderAccessKeyID := "rudderAccessKeyID"
	rudderAccessKey := "rudderAccessKey"
	t.Setenv("RUDDER_AWS_S3_COPY_USER_ACCESS_KEY_ID", rudderAccessKeyID)
	t.Setenv("RUDDER_AWS_S3_COPY_USER_ACCESS_KEY", rudderAccessKey)

	someAccessKeyID := "someAccessKeyID"
	someAccessKey := "someAccessKey"
	someIAMRoleARN := "someIAMRoleARN"
	someWorkspaceID := "someWorkspaceID"

	inputs := []struct {
		destination    *backendconfig.DestinationT
		service        string
		expectedConfig *awsutil.SessionConfig
	}{
		{
			destination: &backendconfig.DestinationT{
				Config: map[string]interface{}{
					"useRudderStorage": true,
				},
			},
			service: "s3",
			expectedConfig: &awsutil.SessionConfig{
				AccessKeyID: rudderAccessKeyID,
				AccessKey:   rudderAccessKey,
				Service:     "s3",
			},
		},
		{
			destination: &backendconfig.DestinationT{
				Config: map[string]interface{}{
					"accessKeyID": someAccessKeyID,
					"accessKey":   someAccessKey,
				},
			},
			service: "glue",
			expectedConfig: &awsutil.SessionConfig{
				AccessKeyID: someAccessKeyID,
				AccessKey:   someAccessKey,
				Service:     "glue",
			},
		},
		{
			destination: &backendconfig.DestinationT{
				Config: map[string]interface{}{
					"iamRoleARN": someIAMRoleARN,
				},
				WorkspaceID: someWorkspaceID,
			},
			service: "redshift",
			expectedConfig: &awsutil.SessionConfig{
				RoleBasedAuth: true,
				IAMRoleARN:    someIAMRoleARN,
				ExternalID:    someWorkspaceID,
				Service:       "redshift",
			},
		},
		{
			destination: &backendconfig.DestinationT{
				Config:      map[string]interface{}{},
				WorkspaceID: someWorkspaceID,
			},
			service: "redshift",
			expectedConfig: &awsutil.SessionConfig{
				AccessKeyID: rudderAccessKeyID,
				AccessKey:   rudderAccessKey,
				Service:     "redshift",
			},
		},
	}
	for _, input := range inputs {
		config, err := CreateAWSSessionConfig(input.destination, input.service)
		require.Nil(t, err)
		require.Equal(t, config, input.expectedConfig)
	}
}

var _ = Describe("Utils", func() {
	DescribeTable("Get columns from table schema", func(schema model.TableSchema, expected []string) {
		columns := GetColumnsFromTableSchema(schema)
		sort.Strings(columns)
		sort.Strings(expected)
		Expect(columns).To(Equal(expected))
	},
		Entry(nil, model.TableSchema{"k1": "v1", "k2": "v2"}, []string{"k1", "k2"}),
		Entry(nil, model.TableSchema{"k2": "v1", "k1": "v2"}, []string{"k2", "k1"}),
	)

	DescribeTable("JSON schema to Map", func(rawMsg json.RawMessage, expected model.Schema) {
		Expect(JSONSchemaToMap(rawMsg)).To(Equal(expected))
	},
		Entry(nil, json.RawMessage(`{"k1": { "k2": "v2" }}`), model.Schema{"k1": {"k2": "v2"}}),
	)

	DescribeTable("Staging table prefix", func(provider string) {
		Expect(StagingTablePrefix(provider)).To(Equal(ToProviderCase(provider, "rudder_staging_")))
	},
		Entry(nil, BQ),
		Entry(nil, RS),
		Entry(nil, SNOWFLAKE),
		Entry(nil, POSTGRES),
		Entry(nil, CLICKHOUSE),
		Entry(nil, MSSQL),
		Entry(nil, AzureSynapse),
		Entry(nil, DELTALAKE),
		Entry(nil, S3Datalake),
		Entry(nil, GCSDatalake),
		Entry(nil, AzureDatalake),
	)

	DescribeTable("Staging table name", func(provider string, limit int) {
		By("Within limits")
		tableName := ToProviderCase(provider, "demo")
		expectedTableName := ToProviderCase(provider, "rudder_staging_demo_")
		Expect(StagingTableName(provider, tableName, limit)).To(HavePrefix(expectedTableName))

		By("Beyond limits")
		tableName = ToProviderCase(provider, "abcdefghijlkmnopqrstuvwxyzabcdefghijlkmnopqrstuvwxyzabcdefghijlkmnopqrstuvwxyzabcdefghijlkmnopqrstuvwxyzabcdefghijlkmnopqrstuvwxyz")
		expectedTableName = ToProviderCase(provider, "rudder_staging_abcdefghijlkmnopqrstuvwxyzabcdefghijlkmnopqrstuvwxyzabcdefghijlkmnopqrstuvwxyzabcdefghijlkmnopqrstuvwxyzabcdefghijlkmnopqrstuvwxyz"[:limit])
		Expect(StagingTableName(provider, tableName, limit)).To(HavePrefix(expectedTableName))
	},
		Entry(nil, BQ, 127),
		Entry(nil, RS, 127),
		Entry(nil, SNOWFLAKE, 127),
		Entry(nil, POSTGRES, 63),
		Entry(nil, CLICKHOUSE, 127),
		Entry(nil, MSSQL, 127),
		Entry(nil, AzureSynapse, 127),
		Entry(nil, DELTALAKE, 127),
		Entry(nil, S3Datalake, 127),
		Entry(nil, GCSDatalake, 127),
		Entry(nil, AzureDatalake, 127),
	)

	DescribeTable("Identity mapping unique mapping constraints name", func(warehouse model.Warehouse, expected string) {
		Expect(IdentityMappingsUniqueMappingConstraintName(warehouse)).To(Equal(expected))
	},
		Entry(nil, model.Warehouse{Namespace: "namespace", Destination: backendconfig.DestinationT{ID: "id"}}, "unique_merge_property_namespace_id"),
	)

	DescribeTable("Identity mapping table name", func(warehouse model.Warehouse, expected string) {
		Expect(IdentityMappingsTableName(warehouse)).To(Equal(expected))
	},
		Entry(nil, model.Warehouse{Namespace: "namespace", Destination: backendconfig.DestinationT{ID: "id"}}, "rudder_identity_mappings_namespace_id"),
	)

	DescribeTable("Identity merge rules table name", func(warehouse model.Warehouse, expected string) {
		Expect(IdentityMergeRulesTableName(warehouse)).To(Equal(expected))
	},
		Entry(nil, model.Warehouse{Namespace: "namespace", Destination: backendconfig.DestinationT{ID: "id"}}, "rudder_identity_merge_rules_namespace_id"),
	)

	DescribeTable("Identity merge rules warehouse table name", func(provider string) {
		Expect(IdentityMergeRulesWarehouseTableName(provider)).To(Equal(ToProviderCase(provider, IdentityMergeRulesTable)))
	},
		Entry(nil, BQ),
		Entry(nil, RS),
		Entry(nil, SNOWFLAKE),
		Entry(nil, POSTGRES),
		Entry(nil, CLICKHOUSE),
		Entry(nil, MSSQL),
		Entry(nil, AzureSynapse),
		Entry(nil, DELTALAKE),
		Entry(nil, S3Datalake),
		Entry(nil, GCSDatalake),
		Entry(nil, AzureDatalake),
	)

	DescribeTable("Identity mappings warehouse table name", func(provider string) {
		Expect(IdentityMappingsWarehouseTableName(provider)).To(Equal(ToProviderCase(provider, IdentityMappingsTable)))
	},
		Entry(nil, BQ),
		Entry(nil, RS),
		Entry(nil, SNOWFLAKE),
		Entry(nil, POSTGRES),
		Entry(nil, CLICKHOUSE),
		Entry(nil, MSSQL),
		Entry(nil, AzureSynapse),
		Entry(nil, DELTALAKE),
		Entry(nil, S3Datalake),
		Entry(nil, GCSDatalake),
		Entry(nil, AzureDatalake),
	)

	DescribeTable("Get object name", func(location string, config interface{}, objectProvider, objectName string) {
		Expect(GetObjectName(location, config, objectProvider)).To(Equal(objectName))
	},
		Entry(GCS, "https://storage.googleapis.com/bucket-name/key", map[string]interface{}{"bucketName": "bucket-name"}, GCS, "key"),
		Entry(S3, "https://bucket-name.s3.amazonaws.com/key", map[string]interface{}{"bucketName": "bucket-name"}, S3, "key"),
		Entry(AzureBlob, "https://account-name.blob.core.windows.net/container-name/key", map[string]interface{}{"containerName": "container-name"}, AzureBlob, "key"),
		Entry(MINIO, "https://minio-endpoint/bucket-name/key", map[string]interface{}{"bucketName": "bucket-name", "useSSL": true, "endPoint": "minio-endpoint"}, MINIO, "key"),
	)

	It("SSL keys", func() {
		destinationID := "destID"
		clientKey, clientCert, serverCA := misc.FastUUID().String(), misc.FastUUID().String(), misc.FastUUID().String()

		err := WriteSSLKeys(backendconfig.DestinationT{ID: destinationID, Config: map[string]interface{}{"clientKey": clientKey, "clientCert": clientCert, "serverCA": serverCA}})
		Expect(err).To(Equal(WriteSSLKeyError{}))

		path := GetSSLKeyDirPath(destinationID)
		Expect(path).NotTo(BeEmpty())

		Expect(os.RemoveAll(path)).NotTo(HaveOccurred())
	})
})

func TestMain(m *testing.M) {
	config.Reset()
	logger.Reset()
	misc.Init()
	Init()
	os.Exit(m.Run())
}
