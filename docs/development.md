# Setup prerequisite environment

## Using docker

1. Install docker

2. Run mariadb instance

```bash
sudo docker run --name undinedb -e MYSQL_ROOT_PASSWORD=password -p 3306:3306 -d mariadb:latest
```

3. Create database to access db

```mysql
CREATE DATABASE undine;
GRANT ALL PRIVILEGES ON undine.* TO 'undine'@'%' IDENTIFIED BY '<PASSWORD>'
```

4. Run RabbitMQ instance

```bash
sudo docker run --name rabbitmq -p 5672:5672 -p 15672:15672 -d rabbitmq:management
```

