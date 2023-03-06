# kafka-udemy-beginner
Udemy beginner course on Spring Kafka


./kafka-console-producer.sh --topic order_created --broker-list localhost:9092 

{"orderId": "626bd1bd-c565-48ac-87b2-28f2247f6dea", "item": "my-new-item"}


./kafka-console-consumer.sh \
--topic order_dispatched \
--bootstrap-server localhost:9092 \
--from-beginning
