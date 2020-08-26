#/bin/bash

# Because we use codebuild as part of codepipeline, following code is required to fix CODEBUILD_SOURCE_VERSION
# Example value of CODEBUILD_SOURCE_VERSION
# CODEBUILD_SOURCE_VERSION="arn:aws:s3:::docker-codebuild-devnull/rudder-server_pr-366/source_out/NAn9t2N.zip"
# Expected value of CODEBUILD_SOURCE_VERSION for pr to be picked correctly is 
# CODEBUILD_SOURCE_VERSION="pr/366"

bash <(curl -s https://codecov.io/bash) -t $CODECOV_TOKEN
