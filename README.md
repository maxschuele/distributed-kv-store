Start leader node:

```sh
go run ./cmd/node -ip 192.168.0.99 -group-port 8080
```

Start replication node:

```sh
go run ./cmd/node -replication -ip 192.168.0.99
```

Start client:

```sh
go run ./cmd/client
```
