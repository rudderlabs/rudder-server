package common

import "slices"

var (
	asyncDestinations = []string{"MARKETO_BULK_UPLOAD", "BINGADS_AUDIENCE", "ELOQUA", "YANDEX_METRICA_OFFLINE_EVENTS"}
	sftpDestinations  = []string{"SFTP"}
)

func IsSFTPDestination(destination string) bool {
	return slices.Contains(sftpDestinations, destination)
}

func IsAsyncRegularDestination(destination string) bool {
	return slices.Contains(asyncDestinations, destination)
}

func IsAsyncDestination(destination string) bool {
	return IsAsyncRegularDestination(destination) || IsSFTPDestination(destination)
}
