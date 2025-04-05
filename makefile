process:
	python3 processes/$(p).py --process $(p)

curl:
	curl http://localhost:8080/sensors | jq

simulation:
	cd client/pp-edge-device && go run main.go
