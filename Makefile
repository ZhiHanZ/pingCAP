build:
    go build -o out/main src/cmd/main.go
run:
    go run src/cmd/main.go
tests:
    go test ./src/pkg/mapreduce/...