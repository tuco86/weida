#!/bin/sh
# /// ci
# needs = ["c-abi-linux-arm64"]
# platform = "linux-arm64"
# image = "go"
# timeout_minutes = 30
# ///
# weida for Go (docs/decisions/0046, docs/libraries/weida-go.md): Go runs
# with cgo enabled, the image's C compiler builds and runs the C smoke program
# against the libweida c-abi-linux-arm64 handed on, and the package links
# its static libweida.a. go test runs the unit tests; the nightly and a
# release tag also run the QUIC tests and the QUIC smoke, built as a binary
# that must not need libweida at run time.
set -eu
. "$CI_WORKSPACE/.zeughaus-ci/build.sh"

step go version
step sh -c 'cgo=$(go env CGO_ENABLED); echo "CGO_ENABLED=$cgo"; [ "$cgo" = 1 ]'
step cc --version
smoke_libweida

cd crates/go/weida
CGO_LDFLAGS="-L$(libweida_input)/lib-static"
GOCACHE="$CI_WORKSPACE/.ci-cache/go-build"
export CGO_LDFLAGS GOCACHE
step go vet -tags integration ./...
case $(tier) in
    ci)
        step go test -count=1 -tags integration ./...
        step go build -o /tmp/weida-smoke ./cmd/weida-smoke
        step sh -c 'if ldd /tmp/weida-smoke | grep libweida; then echo "weida-smoke links libweida dynamically" >&2; exit 1; fi'
        step /tmp/weida-smoke
        ;;
    *) step go test -count=1 ./... ;;
esac
