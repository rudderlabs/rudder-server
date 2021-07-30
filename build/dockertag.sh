#!/bin/bash
UNAME="rudderlabs"
UPASS="$1"
semver="$2"


function docker_tag_exists() {
    TOKEN=$(curl -s -H "Content-Type: application/json" -X POST -d '{"username": "'${UNAME}'", "password": "'${UPASS}'"}' https://hub.docker.com/v2/users/login/ | jq -r .token)
    curl --silent -f --head -lL https://hub.docker.com/v2/repositories/$1/tags/$2/ > /dev/null
}
dockertag=$semver"_internal"
while docker_tag_exists rudderstack/rudder-server-enterprise $dockertag;
do
    cutop=${dockertag##*-}
    if [[ $cutop =~ ^[0-9]+$ ]]; then
        ((cutop++))
        updatedtag=$dockertag
        tagslice="${updatedtag%-*}"
        fulltag=$tagslice"-"$cutop       
        dockertag=${fulltag##*\' \'}
    else
        dtag=$dockertag"-1"
        dockertag=$dtag
    fi
done

echo "${dockertag}"