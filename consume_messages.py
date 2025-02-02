from kafka import KafkaConsumer

bootstrap_servers = 'localhost:9092'
topic = 'diabetes_prediction'

consumer = KafkaConsumer(topic,
                         bootstrap_servers=bootstrap_servers,
			 api_version=(2,0,0),
                         auto_offset_reset='earliest',  
                         enable_auto_commit=False)  

try:
	for message in consumer:
		print(f"Received message: {message.value.decode('utf-8')}")
except KeyboardInterrupt:
	pass
finally:
	# Close the consumer
	consumer.close()
