module forgejo-ci-bridge

go 1.25.1

require (
	code.forgejo.org/forgejo/actions-proto v0.5.2
	connectrpc.com/connect v1.18.1
	github.com/joho/godotenv v1.5.1
	github.com/mattn/go-sqlite3 v1.14.32
	golang.org/x/time v0.13.0
)

require google.golang.org/protobuf v1.36.9 // indirect
