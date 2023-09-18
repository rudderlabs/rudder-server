protoc-gen-go:

```shell
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
```

Generate code: 

```shell
protoc --go_out=. --go_opt=paths=source_relative PulsarApi.proto
```