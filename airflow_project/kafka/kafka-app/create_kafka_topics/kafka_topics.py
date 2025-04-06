from confluent_kafka.admin import AdminClient, NewTopic

user_registration_topic = 'user_registration'
user_search_topic = 'user_search'
foursquare_topic = "foursquare_restaurants"

def create_topic(topic_name, kafka_bootstrap_servers="kafka:9092"):
        """Crea un tópico en Kafka si no existe."""
        admin_client = AdminClient({"bootstrap.servers": kafka_bootstrap_servers})
        topic_list = [NewTopic(topic_name, num_partitions=1, replication_factor=1)]
        future = admin_client.create_topics(topic_list)
        
        for topic, f in future.items():
            try:
                f.result()  # Bloquea hasta que se complete
                print(f"Tópico '{topic}' creado exitosamente.")
            except Exception as e:
                print(f"Advertencia: {e}")

if __name__ == "__main__":
    create_topic(user_registration_topic)
    create_topic(user_search_topic)    
    create_topic(foursquare_topic)  