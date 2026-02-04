Start leader node:

```sh
go run ./cmd/node -ip 192.168.0.99 -http-port 8080 -cluster-port 8081 -group-port 8082 -broadcast-port 9998
```

Start replication node:

```sh
go run ./cmd/node -ip 192.168.0.99 -cluster-port 8083 -broadcast-port 9998
```

Start client:

```sh
go run ./cmd/client
```
