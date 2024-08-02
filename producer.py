from kafka import KafkaProducer
import json
import logging
from faker import Faker
from time import sleep
import uuid
from datetime import datetime

# Configuración de logging
logging.basicConfig(level=logging.INFO)
producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic_name = 'twitter'

# Inicialización de Faker
fake = Faker()

def generate_fake_tweet():
    """Genera un tweet ficticio en formato JSON."""
    return {
        "data": {
            "id": str(uuid.uuid4()),  # Genera un ID único para el tweet
            "text": fake.text(max_nb_chars=280),  # Limita el tamaño del tweet
            "created_at": datetime.utcnow().isoformat() + "Z"  # Fecha y hora actual en formato ISO 8601
        },
        "includes": {
            "users": [
                {
                    "id": str(uuid.uuid4()),  # Genera un ID único para el usuario
                    "name": fake.name(),  # Nombre ficticio del usuario
                    "username": fake.user_name()  # Nombre de usuario ficticio
                }
            ]
        }
    }

def main():
    while True:
        # Generar un tweet ficticio
        tweet = generate_fake_tweet()
        
        # Log para seguimiento
        logging.info(f"Enviando tweet: {tweet}")
        
        # Enviar el tweet a Kafka
        producer.send(topic_name, value=json.dumps(tweet).encode('utf-8'))
        
        # Esperar un poco antes de enviar el siguiente tweet
        sleep(2)  # Ajusta el tiempo de espera según sea necesario

if __name__ == '__main__':
    main()
