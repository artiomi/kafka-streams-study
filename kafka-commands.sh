#!/bin/bash

docker exec -it kafka-kafka1-1 /bin/kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic click-topic

docker exec -it kafka-kafka1-1 /bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic click-topic --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer