# System_Design_Project
The final project for "System Analysis and Design" Course at CE department, Sharif University


### docker compose usage

```python
# create server image
cd server
sudo docker build . -t server_app:latest
```

```python
# create client image
cd client
sudo docker build . -t client_app:latest
```
```python
# run the project
sudo docker compose up -p "sad" -d
```