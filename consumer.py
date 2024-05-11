import cv2
import numpy as np
import pyaudio
from confluent_kafka import Consumer, KafkaError

# Kafka consumer configuration
bootstrap_servers = 'localhost:9092'
video_topic = 'video_topic'
audio_topic = 'audio_topic'
group_id = 'my_consumer_group'  # Specify a unique consumer group ID

# OpenCV video display
cv2.namedWindow('Video', cv2.WINDOW_NORMAL)

# PyAudio audio playback
p = pyaudio.PyAudio()
stream = p.open(format=pyaudio.paInt16, channels=2, rate=44100, output=True)

def consume_video_and_audio():
    consumer = Consumer({
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'  # Start consuming from the beginning of the topic
    })
    consumer.subscribe([video_topic, audio_topic])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # Poll for messages from Kafka
            if msg is None:
                continue  # No new messages, continue polling

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print(f"Reached end of partition: {msg.topic()} [{msg.partition()}]")
                elif msg.error():
                    # Error occurred
                    print(f"Error occurred: {msg.error().str()}")
                continue

            # Process message based on topic
            if msg.topic() == video_topic:
                # Decode video frame
                frame = cv2.imdecode(np.frombuffer(msg.value(), dtype=np.uint8), cv2.IMREAD_COLOR)
                cv2.imshow('Video', frame)
                cv2.waitKey(1)  # Refresh OpenCV window
            
            elif msg.topic() == audio_topic:
                # Play audio
                audio_data = np.frombuffer(msg.value(), dtype=np.int16)
                stream.write(audio_data)

    except KeyboardInterrupt:
        pass

    finally:
        cv2.destroyAllWindows()
        stream.stop_stream()
        stream.close()
        p.terminate()
        consumer.close()

if __name__ == '__main__':
    consume_video_and_audio()
