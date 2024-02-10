cd server
docker build -t server_app:latest -f Dockerfile.broker .
docker build -t zookeeper_app:latest -f Dockerfile.zookeeper .
cd ../client
docker build . -t client_app:latest
cd ..