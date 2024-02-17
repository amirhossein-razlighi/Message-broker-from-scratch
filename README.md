# System_Design_Project (Message Broker - Kafka)
The final project for "System Analysis and Design" Course at CE department, Sharif University. This project is a simple implementation of a message broker using Kafka. The project is implemented in Python and Java. The server is implemented in Python and the client is implemented in both Python and Java. The project also includes CI/CD pipeline using Docker and Docker-compose. We also wrote different unit tests for both client and server that will be explained in the following sections.

## Building docker images
```bash
sudo sh build.sh # or you can use build.bat for windows
```
This command will create and/or update the docker images named as follows: `server_app:latest`, `zookeeper_app:latest`, `client_app:latest`.

# Running the project
```bash
sudo docker compose -p "sad" up -d
```
This command will run the project using docker-compose. This will make (as default, you can change the configuration in compose file) 3 brokers, 1 zookeeper and 2 clients. Also it will configure Prometheus and wrote custom exporters and also used node_exporter to monitor the system and report them using grafana. If you want to only run the project without monitoring, you can use the following command:
```bash
sudo docker compose -p "sad" up -d zookeepers brokers clients
```

## Building Specific Docker Images
You can build the images separately using the following commands:
```bash
# create broker image
cd server
sudo docker build -t server_app:latest -f Dockerfile.broker .
```

```bash
# create zookeeper image
cd server
sudo docker build -t zookeeper_app:latest -f Dockerfile.zookeeper .
```

```bash
# create client image
cd client
sudo docker build . -t client_app:latest
```

## Running Locally with python
You can run the project locally without docker (although it is not recommended) using the following commands:

```bash
cd server
python zookeeper.py --http_port 7500 --socket_port 8000 --ping_port 8888 --host "localhost"
```

```bash
cd server
python broker.py --http_port 7500 --socket_port 8000 --ping_port 8888 --host "localhost"
```

```bash
# Python version
cd client
python client.py
```
Or you can run the Java version of the client using the following commands:
```bash
# Java version
cd client
mvn clean install
java -jar target/client-1.0-SNAPSHOT-jar-with-dependencies.jar
```

## Contribution
Explaining the current structure of the project is as follows:
```
.
-- .github/
---- workflows/
------ deploy.yml
------ integration.yml

-- client/
---- src/
------ main/
-------- java/
---------- com/
------------ example/
-------------- Client.java
-------------- Client.class
---- target/
---- tests/ # Unit tests for the client
---- Dockerfile
---- pom.xml # Maven configuration file for Java 
---- client.py  # Python version of the client

-- prometheus/
---- prometheus.yml # Prometheus configuration file
---- rules.yml # Prometheus rules file

-- server/
---- tests/ # Unit tests for the server
---- broker.py # Python version of the broker
---- zookeeper.py # Python version of the zookeeper
---- Dockerfile.broker
---- Dockerfile.zookeeper
---- metrics.py # Prometheus exporter for the server
---- requirements.txt # Required packages for the server
---- status,py # Enum for the status responses of the server
---- pqueue.py # Priority queue implementation for the server

-- build.sh # Bash script for building the docker images
-- build.bat # Batch script for building the docker images
-- docker-compose.yml # Docker compose file for running the project
-- README.md
-- requirements.txt # Required packages for the client and server in one file