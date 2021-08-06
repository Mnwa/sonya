#!/bin/sh

VERSION=$1

if [ -z "$VERSION" ]
  then
    echo "No argument supplied"
    exit 1
fi

publish_crates_package() {
  PACKAGE=$1
  VERSION=$2

  echo "Publishing on cargo $PACKAGE, version = $VERSION..."

  pushd ./"$PACKAGE" > /dev/null 2>&1
  ACTUAL_VERSION=$(sed -n "s/^.*version = \"\(.*\)\".*$/\1/p" Cargo.toml)
  if [ "$ACTUAL_VERSION" == "$VERSION" ]; then
    cargo publish
  else
    echo "Wrong package version in ./$PACKAGE/Cargo.toml, expected version = $VERSION, actual_version = $ACTUAL_VERSION"
    exit 1
  fi
  popd > /dev/null 2>&1
}

publish_docker_package() {
  PACKAGE=$1
  VERSION=$2
  TAG=$3

  echo "Publishing on docker $PACKAGE, version = $VERSION..."

  docker build -t mnwamnowich/web-queue:"$TAG"-latest -t mnwamnowich/web-queue:"$TAG"-"$VERSION" -f "$PACKAGE"/Dockerfile .
}

publish_crates_package "web-queue-meta" "$VERSION"
publish_crates_package "web-queue-server" "$VERSION"
publish_crates_package "web-queue-backend" "$VERSION"

publish_docker_package "web-queue-server" "$VERSION" "backend"
publish_docker_package "web-queue-proxy" "$VERSION" "proxy"
