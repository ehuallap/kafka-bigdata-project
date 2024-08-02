from kafka import KafkaConsumer
import json
import logging
import re
from hdfs import InsecureClient

# Configuración de logging
logging.basicConfig(level=logging.INFO)

# Configuración de Kafka
consumer = KafkaConsumer(
    'twitter',
    bootstrap_servers='localhost:9093',
    group_id='twitter-consumer-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Configuración de HDFS
hdfs_client = InsecureClient('http://localhost:50070', user='hadoop')

# Crear directorio en HDFS si no existe
hdfs_client.makedirs('/data')

# Definición de categorías y palabras clave
CATEGORIES = {
    'religion': ['god', 'church', 'faith', 'bible', 'pray'],
    'food': ['pizza', 'burger', 'sushi', 'coffee', 'restaurant'],
    'drinks': ['beer', 'wine', 'cocktail', 'juice', 'water'],
    'travel': ['vacation', 'trip', 'flight', 'hotel', 'destination'],
    'sports': ['football', 'basketball', 'soccer', 'tennis', 'exercise'],
    'technology': ['computer', 'software', 'hardware', 'internet', 'tech'],
    'movies': ['film', 'cinema', 'actor', 'director', 'trailer'],
    'music': ['song', 'album', 'concert', 'band', 'artist'],
    'fashion': ['clothing', 'shoes', 'accessories', 'style', 'design'],
    'health': ['medicine', 'doctor', 'fitness', 'diet', 'wellness']
}

def categorize_tweet(text):
    """Categoriza un tweet basado en palabras clave."""
    for category, keywords in CATEGORIES.items():
        if any(re.search(r'\b' + re.escape(keyword) + r'\b', text, re.IGNORECASE) for keyword in keywords):
            return category
    return 'unknown'  # Categoría por defecto si no se encuentra ninguna coincidencia

def main():
    tweet_buffer = []
    file_index = 1
    
    for message in consumer:
        tweet = message.value
        tweet_text = tweet.get('data', {}).get('text', '')
        
        # Categorizar el tweet
        category = categorize_tweet(tweet_text)
        
        # Mostrar el tweet y su categoría
        logging.info(f"Tweet: {tweet_text}")
        logging.info(f"Categoría: {category}")
        
        # Agregar tweet al buffer
        tweet_buffer.append(f"Tweet: {tweet_text}\nCategoría: {category}\n\n")
        
        # Si hay 2 tweets en el buffer, escribirlos en un archivo en HDFS
        if len(tweet_buffer) == 2:
            file_name = f'user/data/file_{file_index}.txt'
            try:
                with hdfs_client.write(file_name, encoding='utf-8') as writer:
                    writer.write(''.join(tweet_buffer))
                logging.info(f"Guardado en HDFS: {file_name}")
            except Exception as e:
                logging.error(f"Error al escribir en HDFS: {e}")
            
            # Limpiar el buffer y actualizar el índice del archivo
            tweet_buffer.clear()
            file_index += 1

if __name__ == '__main__':
    main()
