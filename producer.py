from moviepy.editor import VideoFileClip
from confluent_kafka import Producer
import cv2
import os

def publish_video_with_audio(video_file, video_topic, audio_topic):
    # Create Kafka Producer
    producer = Producer({'bootstrap.servers': 'localhost:9092','compression.type': 'gzip'})

    try:
        # Extract audio from video file
        video_clip = VideoFileClip(video_file)
        audio_file = "audio.wav"  # Temporarily save audio to a file
        video_clip.audio.write_audiofile(audio_file)

        # Publish video frames to video_topic
        batch_size = 10  # Number of frames to batch before publishing
        frame_buffer = []
        for frame in video_clip.iter_frames():
            _, img_encoded = cv2.imencode('.jpg', frame)
            img_bytes = img_encoded.tobytes()
            frame_buffer.append(img_bytes)
            if len(frame_buffer) >= batch_size:
                for img in frame_buffer:
                    producer.produce(video_topic, value=img)
                producer.flush()  # Flush after each batch
                frame_buffer = []

        # Publish remaining frames in buffer
        for img in frame_buffer:
            producer.produce(video_topic, value=img)

        # Publish audio to audio_topic
        with open(audio_file, 'rb') as f:
            audio_data = f.read()
            producer.produce(audio_topic, value=audio_data)

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Cleanup
        if os.path.exists(audio_file):
            os.remove(audio_file)

        # Flush and poll messages to ensure all are delivered
        producer.flush()
        producer.poll(0)  # Handle message callbacks
        video_clip.close()

if __name__ == '__main__':
    video_file = 'lofi.mp4'
    video_topic = 'video_topic'
    audio_topic = 'audio_topic'
    publish_video_with_audio(video_file, video_topic, audio_topic)
