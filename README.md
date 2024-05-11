# Video Streaming

**Prerequisites**

Docker and Docker Compose installed on your system.

*How to Run*

  **Clone the Repository:**
  
    git clone https://github.com/morshed-sarwer/video-streaming.git
  
  **Start the Docker Compose Environment:**
  
    docker-compose up -d
  
  **Access Control Center UI:**
  
    Open your web browser and go to http://localhost:9021 to access the Control Center UI for monitoring your Kafka cluster.
  
  **Run Producer Script:**
    
    python producer.py
  
  **Run Consumer Script:**
    
    python consumer.py
