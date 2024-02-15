# System_Design_Project
The final project for "System Analysis and Design" Course at CE department, Sharif University

## Running the project
```python
# run server
cd server
python3 broker.py --http_port 8000 --socket_port 8001 --host 127.0.0.1
```
```python
# run client
!cd client
python3 client.py
```
## Running Unit Tests
```python
!cd server & cd tests
python3 test_x.py # write the name of the test file
```
## Docker compose usage

```python
# create broker image
cd server
sudo docker build -t server_app:latest -f Dockerfile.broker .
```

```python
# create zookeeper image
cd server
sudo docker build -t zookeeper_app:latest -f Dockerfile.zookeeper .
```

```python
# create client image
cd client
sudo docker build . -t client_app:latest
```
```python
# run the project
sudo docker compose -p "sad" up -d
```

## Running Client
```python
# Python
cd client
python client.py
```
```java
// Java
cd client
mvn clean install
java -jar target/client-1.0-SNAPSHOT-jar-with-dependencies.jar
```
