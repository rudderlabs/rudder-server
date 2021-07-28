#!/bin/bash
tag="$1"
tagslice="${tag%?}"
UNAME="rudderlabs"
UPASS="$2"
semver="$3"
buildname="$4"


function docker_tag_exists() {
    TOKEN=$(curl -s -H "Content-Type: application/json" -X POST -d '{"username": "'${UNAME}'", "password": "'${UPASS}'"}' https://hub.docker.com/v2/users/login/ | jq -r .token)
    curl --silent -f --head -lL https://hub.docker.com/v2/repositories/$1/tags/$2/ > /dev/null
}

if [ $buildname == "enterprise" ]; then
    dockertag=$semver"-internal"
    while docker_tag_exists rudderstack/rudder-server $dockertag;
    do
        cutop=${dockertag: -1}
        if [[ $cutop =~ ^[+-]?[0-9]+$ ]]; then
            ((cutop++))
            dockertagin=$dockertag"-1"
            tagslice="${dockertagin%?}"
            fulltag=$semver"-"$tagslice$cutop       
            dockertag=${fulltag##*\' \'}
        else
            dtag=$dockertag"-1"
            dockertag=$dtag
        fi
    done
else
    dockertag=$semver"-"$tag
    while docker_tag_exists rudderlabs/rudder-server $dockertag;
    do
        cutop=${dockertag: -1}
        if [[ $cutop =~ ^[+-]?[0-9]+$ ]]; then
            ((cutop++))
            dockertag=$tag"-1"
            tagslice="${dockertag%?}"
            fulltag=$semver"-"$tagslice$cutop       
            dockertag=${fulltag##*\' \'}
        else
            dtag=$dockertag"-1"
            dockertag=$dtag
        fi
    done
fi

echo "${dockertag}"