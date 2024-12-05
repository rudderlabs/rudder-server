package common

import "slices"

var (
	asyncDestinations = []string{"MARKETO_BULK_UPLOAD", "BINGADS_AUDIENCE", "ELOQUA", "YANDEX_METRICA_OFFLINE_EVENTS", "BINGADS_OFFLINE_CONVERSIONS", "KLAVIYO_BULK_UPLOAD", "LYTICS_BULK_UPLOAD", "SNOWPIPE_STREAMING"}
	sftpDestinations  = []string{"SFTP"}
)

func IsSFTPDestination(destination string) bool {
	return slices.Contains(sftpDestinations, destination)
}

func IsAsyncRegularDestination(destination string) bool {
	return slices.Contains(asyncDestinations, destination)
}

func IsAsyncDestination(destination string) bool {
	return slices.Contains(append(asyncDestinations, sftpDestinations...), destination)
}
