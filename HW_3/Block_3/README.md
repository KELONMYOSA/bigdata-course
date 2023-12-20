#### Переходим в директорию с docker-compose

```commandline
cd .\HW_3\kafka-flink-hdfs-docker\
```

#### Создаем топики в Kafka

```commandline
docker-compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --create --topic block3 --partitions 3 --replication-factor 1
```

```commandline
docker-compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --create --topic block3-processed --partitions 3 --replication-factor 1
```

#### Передаем джобу в Flink

```commandline
docker-compose exec jobmanager ./bin/flink run -py /opt/pyflink/Block_3/device_job.py -d
```

#### Запускаем producer и consumer

```commandline
cd ..\Block_3\
```

```commandline
python producer.py
```

```commandline
python consumer.py
```

#### Если установить вероятность подключения 0 и стандартные параметры backoff

```commandline
python consumer.py 
Connecting to Kafka brokers
Attempt 1 out of 5:
Database connection error!
Attempt 2 out of 5:
Database connection error!
Attempt 3 out of 5:
Database connection error!
Attempt 4 out of 5:
Database connection error!
Attempt 5 out of 5:
Database connection error!
Traceback (most recent call last):
  File "C:\Users\KELONMYOSA\Desktop\Study\bigdata-course\HW_3\Block_3\consumer.py", line 51, in <module>
    create_consumer()
  File "C:\Users\KELONMYOSA\Desktop\Study\bigdata-course\HW_3\Block_3\consumer.py", line 47, in create_consumer
    message_handler(message)
  File "C:\Users\KELONMYOSA\Desktop\Study\bigdata-course\HW_3\Block_3\consumer.py", line 22, in _wrapper
    raise Exception("All attempts failed!")
Exception: All attempts failed!

Process finished with exit code 1
```

#### Если установить вероятность подключения 0.8 и стандартные параметры backoff

```commandline
python consumer.py 
Connecting to Kafka brokers
ConsumerRecord(topic='block3-processed', partition=2, offset=151, timestamp=1703087648140, timestamp_type=0, key=None, value=b"{'device_id': 3, 'temperature': 78.67735089884889, 'execution_time': 2165}", headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=74, serialized_header_size=-1)
ConsumerRecord(topic='block3-processed', partition=2, offset=152, timestamp=1703087649141, timestamp_type=0, key=None, value=b"{'device_id': 8, 'temperature': 83.75407395429096, 'execution_time': 2170}", headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=74, serialized_header_size=-1)
ConsumerRecord(topic='block3-processed', partition=2, offset=153, timestamp=1703087654146, timestamp_type=0, key=None, value=b"{'device_id': 1, 'temperature': 97.43810855223455, 'execution_time': 2195}", headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=74, serialized_header_size=-1)
Attempt 1 out of 5:
Database connection error!
ConsumerRecord(topic='block3-processed', partition=2, offset=154, timestamp=1703087655147, timestamp_type=0, key=None, value=b"{'device_id': 2, 'temperature': 100.41166292733391, 'execution_time': 2200}", headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=75, serialized_header_size=-1)
ConsumerRecord(topic='block3-processed', partition=2, offset=155, timestamp=1703087660153, timestamp_type=0, key=None, value=b"{'device_id': 4, 'temperature': 66.40840587853677, 'execution_time': 2225}", headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=74, serialized_header_size=-1)
Attempt 1 out of 5:
Database connection error!
Attempt 2 out of 5:
Database connection error!
ConsumerRecord(topic='block3-processed', partition=2, offset=156, timestamp=1703087661153, timestamp_type=0, key=None, value=b"{'device_id': 9, 'temperature': 77.60163062002346, 'execution_time': 2230}", headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=74, serialized_header_size=-1)
ConsumerRecord(topic='block3-processed', partition=2, offset=157, timestamp=1703087664156, timestamp_type=0, key=None, value=b"{'device_id': 10, 'temperature': 83.65200730787734, 'execution_time': 2245}", headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=75, serialized_header_size=-1)
Attempt 1 out of 5:
Database connection error!
ConsumerRecord(topic='block3-processed', partition=2, offset=158, timestamp=1703087665156, timestamp_type=0, key=None, value=b"{'device_id': 7, 'temperature': 79.92946558869357, 'execution_time': 2250}", headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=74, serialized_header_size=-1)
```