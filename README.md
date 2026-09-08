# 🃏 Joke Kafka Consumer


[![🇬🇧 English](https://img.shields.io/badge/🇬🇧_English-README-blue?style=for-the-badge&logo=markdown&logoColor=white)](./README.md)
[![🇷🇺 Русский](https://img.shields.io/badge/🇷🇺_Русский-README-red?style=for-the-badge&logo=markdown&logoColor=white)](./README.ru.md)

---
https://img.shields.io/badge/license-MIT-blue.svg
https://img.shields.io/badge/Java-17%252B-orange
https://img.shields.io/badge/Spring%2520Boot-3.4.0-brightgreen
https://img.shields.io/badge/Apache%2520Kafka-3.8.0-black


<img src="https://img.shields.io/badge/Java-ED8B00?style=for-the-badge&logo=java&logoColor=white" /> <img src="https://img.shields.io/badge/Spring_Boot-6DB33F?style=for-the-badge&logo=spring-boot&logoColor=white" /> <img src="https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white" /> <img src="https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white" /> <img src="https://img.shields.io/badge/MySQL-4479A1?style=for-the-badge&logo=mysql&logoColor=white" /> <img src="https://img.shields.io/badge/H2-004027?style=for-the-badge&logo=h2&logoColor=white" /> <img src="https://img.shields.io/badge/Maven-C71A36?style=for-the-badge&logo=apache-maven&logoColor=white" />



Joke Consumer — это сервис-потребитель Apache Kafka, который читает сообщения с шутками из топика и сохраняет их в базу данных. Проект написан на Java с использованием Spring Boot и предназначен для использования в качестве микросервиса в системах обработки потоковых данных.


## 📚 Table of Contents

  * [About the Project](#About-the-Project)

  *  [Architecture and Data Flow](#Architecture-and-Data-Flow)

  *  [Key Features](#Key-Features)

  *  [Technology Stack](#Technology-Stack)

  *  [Getting Started](#Getting-Started)

  *  [Prerequisites](#Prerequisites)

  *  [Installation and Setup](#Installation-and-Setup)

  *  [Configuration](#Configuration)

  *  [Testing](#Testing)

  *  [License](#License)

  *  [Contacts](#Contacts)

## 🎯 
## About the Project

The Joke Consumer project is part of a joke processing system. It subscribes to a specified Kafka topic, receives joke messages (in JSON format), deserializes them, performs necessary validation, and saves them to a relational database for further use.

This service is an example implementation of the "Competing Consumers" pattern and can be easily scaled horizontally to increase throughput.


## 📊 
## Architecture and Data Flow

<img width="1718" height="357" alt="image" src="https://github.com/user-attachments/assets/61d580a7-6f78-441c-a42a-e9a6c48f768d" />





### Data Flow:

* Producer sends a joke message to the Kafka topic.

* Joke Consumer listens to the topic and receives a new message.

* The service deserializes the JSON into a domain model object.

* Data validation is performed (e.g., checking for text and joke type).

* Valid jokes are saved to the database.

* In case of errors, the message may be sent to DLQ (Dead Letter Queue) or logged.

---

## ✨ 
## Key Features

* Reading from Kafka: Connects to a Kafka cluster and reads messages from the specified topic.

* Saving to Database: JPA support for saving received data to a relational database (PostgreSQL/MySQL/H2).

* Error Handling: Basic retry mechanisms and error logging.

* Configurability: Flexible configuration via application.yml (broker addresses, topic, consumer group).

* Easy Launch: Build with Maven and run as a standalone Spring Boot application.

## 🛠 
## Technology Stack

|Component	|Technology	|Version|
|-------------------|----------|------------------|
|Language	|Java|	17+|
|Framework	|Spring Boot|	3.4.0|
|Kafka Client|	Spring Kafka|	3.3.0|
|Database Access|	Spring Data JPA|	-|
|Build Tool	|Maven|	3.9.0+|
|Message Broker	|Apache Kafka|3.8.0|
|Database|	PostgreSQL / MySQL / H2 (default H2 for development)|	-|
|Documentation|	Mermaid for diagrams|	-|

## 🚀 
## Getting Started
### Prerequisites

#### To successfully run the project, ensure you have installed:

* Java Development Kit (JDK) version 17 or higher.

* Apache Maven version 3.9.0 or higher.

* Apache Kafka cluster (local or remote) with a jokes topic created.

* Database Management System (PostgreSQL/MySQL) or use the embedded H2 for testing.

### Installation and Setup

#### 1. Clone the repository:

```bach
git clone https://github.com/sergeyh510-alt/Joke-consumer.git
cd Joke-consume
```
#### 2. Configure connection to Kafka and the database in src/main/resources/application.yml. Example base configuration:
```bach
spring:
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: joke-consumer-group
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "*"
        spring.json.type.mapping: joke:com.example.jokeconsumer.model.Joke
  datasource:
    url: jdbc:h2:mem:jokedb
    driver-class-name: org.h2.Driver
    username: sa
    password:
  jpa:
    database-platform: org.hibernate.dialect.H2Dialect
    hibernate.ddl-auto: update
```
#### 3.Build the project with Maven:
```bach
./mvnw clean package
```
#### 4.Run the application:
```bach
./mvnw spring-boot:run
```
#### Or run the built JAR file:
```bach
java -jar target/joke-consumer-0.0.1-SNAPSHOT.jar
```
## ⚙️ 
## Configuration 
#### Key configuration parameters are defined in application.yml. The main ones are:

|Parameter|	Description|	Default Value|
|-----------------------------|-------------------------|------------------------------|
|spring.kafka.bootstrap-servers|	Kafka broker| addresses	localhost:9092|
|spring.kafka.consumer.group-id	|Consumer group ID	|joke-consumer-group|
|spring.kafka.consumer.auto-offset-reset|	Offset reset strategy (earliest/latest)|	earliest
|spring.datasource.url|	Database connection URL|	jdbc:h2:mem:jokedb|
|joke.topic.name (add yourself)	|Topic name for reading jokes	|jokes|


#### For production environments, it is recommended to override these parameters using environment variables or an external configuration file.


## 🧪 
## Testing
#### The project includes basic unit tests to verify:

* Message deserialization.

* Repository save logic.

* Error handling.

#### To run tests, execute:
```bach
./mvnw test
```
#### For more advanced testing with a real Kafka instance, you can use Testcontainers.

## 📄 
## License
This project is distributed under the MIT License. See the LICENSE file for details.

#### Joke Consumer — a simple but functional example of a Kafka consumer microservice.
#### I hope this project will be useful for learning or as a foundation for your own solutions 😄

### 📞 
### Contacts
* Contact: Sergey Chekryzhov
* Email: sergeyh510@gmail.com
* GitHub: sergeyh510-alt
* Project: Joke Kafka Consumer
* LinkedIn: www.linkedin.com/in/sergey-chekryzhov-a38778217
* Telegram: @SergeyChekryzhov
