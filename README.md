```
go run ./cmd/node -http :8080 -group :8081 -leader -members ":8083,:8085"
go run ./cmd/node -http :8082 -group :8083 -members ":8085" -leader-addr :8081
go run ./cmd/node -http :8084 -group :8085 -members ":8083" -leader-addr :8081
```

```sh
curl -X PUT "http://localhost:8080/kv?key=mykey" -d "myvalue"
curl "http://localhost:8084/kv?key=mykey"
```
