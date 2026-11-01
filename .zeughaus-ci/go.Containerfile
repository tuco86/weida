# The image the go-linux-* jobs run in: Go 1.27 with cgo, for the cgo
# package over libweida (docs/decisions/0046). golang:1.27-trixie carries
# gcc, which cgo and build.sh's smoke_libweida need, and trixie's glibc,
# which libweida is built against.
# The same file serves amd64 under podman and arm64 under Apple `container`.
FROM docker.io/library/golang:1.27-trixie
