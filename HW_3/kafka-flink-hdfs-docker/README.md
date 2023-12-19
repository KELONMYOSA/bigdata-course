# Hadoop
(Почти) настроенный докер с последним hadoop и сопутствующими инструментами на борту

Образ при сборке выкачивает много данных (ставит хадупы\юпитеры\хайвы и т.д.). Это норма.
Лучше не запускаться при подключении к лимитному интернету.

Для запуска:

1. Поставить docker + docker-compose на локальную машину

Для запуска hadoop:
1. Сначала запускаем неймноду с командой command: ["hdfs", "namenode", "-format", "-force"] 
2. Так запуститься надо только в первый раз (либо, после того, как вы снесли образ и примонтированный раздел)
3. После того как контейнер отработал и завершился, запускаемся с командой command: ["hdfs", "namenode"]
4. После неймноды поднимаем датаноды, нодменеджеры и т.д.

# Kafka

```commandline
docker-compose build
```

```commandline
docker-compose up -d
```

```commandline
docker-compose ps
```

```
http://localhost:8081/#/overview
```

```commandline
docker-compose down -v
```

```commandline
docker-compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --create --topic topic_name --partitions 1 --replication-factor 1
```

```commandline
docker-compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --describe itmo  
```

```commandline
 docker-compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --alter --topic itmo --partitions 2
```

```commandline
docker-compose exec jobmanager ./bin/flink run -py /opt/pyflink/device_job.py -d  
```