if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <overrides_existing_version>" >&2
  exit 1
fi

read -p "Are you sure to release? " -n 1 -r
echo    # (optional) move to a new line
if [[ $REPLY =~ ^[Yy]$ ]]
then
  git tag -d $1
  git push origin :refs/tags/$1

  git tag -a $1 -m "Release $1"
  git push origin $1

  goreleaser --rm-dist
fi
