#!/bin/bash

# Used mysql docker entrypoint script as reference to create this script
# https://github.com/docker-library/mysql/blob/master/5.6/docker-entrypoint.sh

# check to see if this file is being run or sourced from another script
_is_sourced() {
	# https://unix.stackexchange.com/a/215279
	[ "${#FUNCNAME[@]}" -ge 2 ] \
		&& [ "${FUNCNAME[0]}" = '_is_sourced' ] \
		&& [ "${FUNCNAME[1]}" = 'source' ]
}

_main() {
    echo "Executing docker entrypoint script"
    mkdir -p $RUDDER_TMPDIR 2>/dev/null
    if [ "$COMPUTE_DB_HOST_IN_K8S" = true ]; then
        _pod_index=${HOSTNAME##*-}
        if [ -z ${POSTGRES_POD_NAME} ]; then echo "POSTGRES_POD_NAME env variable is required"; exit 1; fi
        if [ -z ${POSTGRES_HEADLESS_SVC} ]; then echo "POSTGRES_HEADLESS_SVC env variable is required"; exit 1; fi
        _target_postgres_pod="$POSTGRES_POD_NAME-$_pod_index"
        export JOBS_DB_HOST="$_target_postgres_pod.$POSTGRES_HEADLESS_SVC"
        echo "Computed db host to $JOBS_DB_HOST"
    fi
	exec "$@"
}

# If we are sourced from elsewhere, don't perform any further actions
if ! _is_sourced; then
	_main "$@"
fi
