#!/bin/sh

VERSION=$1

if [ -z "$VERSION" ]; then
  echo "No argument supplied"
  exit 1
fi

validate_crates_package() {
  PACKAGE=$1
  VERSION=$2

  echo "Validating cargo package $PACKAGE, version = $VERSION..."

  pushd ./"$PACKAGE" >/dev/null 2>&1
  ACTUAL_VERSION=$(sed -n "s/^version = \"\([0-9.]*\)\"$/\1/p" Cargo.toml)
  if [ "$ACTUAL_VERSION" != "$VERSION" ]; then
    echo "Wrong package version in ./$PACKAGE/Cargo.toml, expected version = $VERSION, actual_version = $ACTUAL_VERSION"
    exit 1
  fi
  popd >/dev/null 2>&1
}

publish_crates_package() {
  PACKAGE=$1
  VERSION=$2

  echo "Publishing on cargo $PACKAGE, version = $VERSION..."

  pushd ./"$PACKAGE" >/dev/null 2>&1
  cargo publish
  popd >/dev/null 2>&1
}

publish_docker_package() {
  PACKAGE=$1
  VERSION=$2
  TAG=$3

  echo "Publishing on docker $PACKAGE, version = $VERSION..."

  DOCKER_LATEST_TAG="mnwamnowich/sonya:$TAG-latest"
  DOCKER_VERSION_TAG="mnwamnowich/sonya:$TAG-$VERSION"

  docker build -t $DOCKER_LATEST_TAG -t $DOCKER_VERSION_TAG -f "$PACKAGE"/Dockerfile .
  docker push $DOCKER_VERSION_TAG
  docker push $DOCKER_LATEST_TAG
}

validate_crates_package "sonya-meta" "$VERSION"
validate_crates_package "sonya" "$VERSION"
validate_crates_package "sonya-proxy" "$VERSION"

publish_crates_package "sonya-meta" "$VERSION"
publish_crates_package "sonya" "$VERSION"
publish_crates_package "sonya-proxy" "$VERSION"

publish_docker_package "sonya" "$VERSION" "queue"
publish_docker_package "sonya-proxy" "$VERSION" "proxy"
