cd server
sudo docker build -t server_app:latest -f Dockerfile.broker .
sudo docker build -t zookeeper_app:latest -f Dockerfile.zookeeper .
cd ../client
sudo docker build -t client_app:latest .
cd ..
