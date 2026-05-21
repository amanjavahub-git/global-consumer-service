# Global Consumer Service (Kafka + Spring Boot)

## Overview
This project is a Spring Boot microservice that acts as both a Kafka producer and consumer for pharmacy orders.  
It demonstrates:
- Manual offset commit (`AckMode.MANUAL`)
- REST APIs for producing and consuming messages
- Offset tracking per partition
- Swagger UI for API documentation

## Requirements
- Java 17
- Maven 3.8+
- Apache Kafka (local or remote cluster)

## Setup
1. Clone the repository:
   ```bash
   git clone https://github.com/your-org/global-consumer-service.git
   cd global-consumer-service
