
# 📈 Kafka-Based Stock Market Streaming Data Pipeline

This project implements a real-time stock market data simulation using **Kafka**, with the data flowing through **EC2**, **S3**, **AWS Glue**, and **Amazon Athena** for analysis.
## Project Architecture
![PROJECT_ARCH](https://github.com/user-attachments/assets/f94e11d3-8c00-4525-9974-afbbefd6fbc0)

---

## 🖥️ EC2 + Kafka Setup

### Step 1: Connect to EC2

1. Go to the EC2 dashboard in AWS Console.
2. Copy the **SSH client** string from the instance details.
3. Open Git Bash on your local machine.
4. Navigate to the folder where your EC2 key pair `.pem` file is saved.
5. Run the SSH command to connect:
```bash
ssh -i kafka-stock-market-project.pem ec2-user@<your-ec2-public-ip>
```

### Step 2: Download and Setup Kafka

```bash
wget https://archive.apache.org/dist/kafka/3.3.1/kafka_2.12-3.3.1.tgz
tar -xvf kafka_2.12-3.3.1.tgz
cd kafka_2.12-3.3.1
```

### Step 3: Install Java

```bash
sudo dnf install java-17-amazon-corretto -y
java -version
```

### Step 4: Start Zookeeper

```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

### Step 5: Start Kafka Server (in a new terminal session)

```bash
export KAFKA_HEAP_OPTS="-Xmx256M -Xms128M"
cd kafka_2.12-3.3.1
bin/kafka-server-start.sh config/server.properties
```

### Tip (Run Zookeeper in Background)

```bash
nohup bin/zookeeper-server-start.sh config/zookeeper.properties > zookeeper.log 2>&1 &
jps  # Check if Zookeeper and Kafka are running
```

---

## 🔄 Configure Kafka for External Access

Update `config/server.properties`:

```properties
broker.id=0
log.dirs=/tmp/kafka-logs
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://<your-ec2-public-ip>:9092
zookeeper.connect=localhost:2181
zookeeper.connection.timeout.ms=6000
```

Save and exit (Ctrl+O, Enter, Ctrl+X)

---

## ✅ Allow Port 9092 in Security Group

1. Go to EC2 > Security Groups.
2. Edit Inbound Rules:
   - Type: Custom TCP
   - Port: 9092
   - Source: 0.0.0.0/0 (or your IP)

---

## 🧪 Test Kafka

### Create Kafka Topic

```bash
bin/kafka-topics.sh --create --topic demo_testing1 --bootstrap-server <EC2_PUBLIC_IP>:9092 --replication-factor 1 --partitions 1
```

### Start Producer

```bash
bin/kafka-console-producer.sh --topic demo_testing1 --bootstrap-server <EC2_PUBLIC_IP>:9092
```

### Start Consumer

```bash
bin/kafka-console-consumer.sh --topic demo_testing1 --bootstrap-server <EC2_PUBLIC_IP>:9092
```

---

## 💻 Python & Jupyter on Local Machine

### Setup Virtual Environment

```bash
python -m venv jupyter_env
jupyter_env\Scripts\activate
pip install --upgrade pip
pip install notebook
jupyter notebook
```

---

## 🐍 Producer-Consumer Pipeline with S3

- Producer sends data from API to Kafka — Use `KafkaProducer.ipynb`
- Consumer stores Kafka data to S3 — Use `KafkaConsumer.ipynb`

---

## 🪣 Create S3 Bucket

1. Go to S3 Console.
2. Click **Create bucket**.
3. Name: `kafka-stock-market-analysis-yeshwanth`
4. Region: `ap-south-1`
5. Grant **Administrator access** during creation.
6. Click Create.

---

## 🔐 Setup IAM & AWS CLI

1. Download `.csv` file with Access Key ID and Secret Access Key
2. Install AWS CLI
3. Configure via terminal:

```bash
aws configure
# Paste credentials
```

---

## 🧹 AWS Glue + Athena Integration

### 1. Create Glue Crawler

- Data source: `s3://kafka-stock-market-analysis-yeshwanth/`
- IAM role: `glue-admin-role`
- Database: `kafka-stock-market-analysis-yeshwanth`

### 2. Run Crawler

- This populates AWS Glue Data Catalog.

### 3. Query with Athena

- Go to Amazon Athena Console
- Set database to `kafka-stock-market-analysis-yeshwanth`
- Run SQL queries on your JSON data

---

## ✅ End-to-End Flow

1. Producer fetches data from API
2. Kafka streams it to a topic
3. Consumer stores messages to S3
4. AWS Glue catalogs the data
5. Athena allows querying the stock data

---

## 🧾 Sample Kafka Topic Creation

```bash
bin/kafka-topics.sh --create --topic demo_testing1 --bootstrap-server 13.233.92.65:9092 --replication-factor 1 --partitions 1
```

---

## 🔐 Credentials (Example)

```
Access Key ID: AKIAXSXH4E5CTTLEZCQE
Secret Access Key: qrh+jff/R6hv+yLw9QzDFlF5lYYQAiQ3dwlV0eGZ
```

---

## 🧼 Clean Restart (If Needed)

```bash
pkill -f kafka.Kafka
pkill -f zookeeper
rm -rf /tmp/kafka-logs /tmp/zookeeper
rm -rf logs/*
```

---

## 📌 Note

Make sure your EC2 instance’s public IP is consistent or use Elastic IP to avoid reconfiguring.

---
