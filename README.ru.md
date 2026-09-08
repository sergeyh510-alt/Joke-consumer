# Joke-consumer


[![🇬🇧 English](https://img.shields.io/badge/🇬🇧_English-README-blue?style=for-the-badge&logo=markdown&logoColor=white)](./README.md)
[![🇷🇺 Русский](https://img.shields.io/badge/🇷🇺_Русский-README-red?style=for-the-badge&logo=markdown&logoColor=white)](./README.ru.md)

---


# 🃏 Joke Kafka Consumer

Сервис-потребитель (Consumer) для асинхронного приёма шуток из Apache Kafka и их сохранения в PostgreSQL.

---

## 📌 Оглавление

1. [Описание проекта](#описание-проекта)
2. [Архитектура и компоненты](#архитектура-и-компоненты)
   - [Диаграмма компонентов](#диаграмма-компонентов)
   - [Диаграмма последовательности](#диаграмма-последовательности)
   - [ER-диаграмма](#er-диаграмма)
3. [Технологический стек](#технологический-стек)
4. [Структура проекта](#структура-проекта)
5. [Установка и запуск](#установка-и-запуск)
   - [Предварительные требования](#предварительные-требования)
   - [Настройка окружения (Docker Compose)](#настройка-окружения-docker-compose)
   - [Настройка приложения](#настройка-приложения)
   - [Сборка и запуск](#сборка-и-запуск)
6. [Конфигурация](#конфигурация)
   - [Kafka Consumer](#kafka-consumer)
   - [База данных](#база-данных)
   - [Логирование](#логирование)
7. [REST API (для мониторинга)](#rest-api-для-мониторинга)
   - [Получить все шутки](#получить-все-шутки)
   - [Получить последние шутки](#получить-последние-шутки)
   - [Статистика работы consumer](#статистика-работы-consumer)
   - [Поиск по ключевому слову](#поиск-по-ключевому-слову)
8. [Особенности реализации](#особенности-реализации)
   - [Дедупликация сообщений](#дедупликация-сообщений)
   - [Ручное подтверждение offset (Manual Ack)](#ручное-подтверждение-offset-manual-ack)
   - [Обработка ошибок](#обработка-ошибок)
   - [Транзакционность](#транзакционность)
   - [Сохранение метаданных Kafka](#сохранение-метаданных-kafka)
9. [Тестирование](#тестирование)
   - [Интеграционное тестирование с Kafka](#интеграционное-тестирование-с-kafka)
   - [Ручное тестирование](#ручное-тестирование)
10. [Примеры работы](#примеры-работы)
11. [Возможные проблемы и решения](#возможные-проблемы-и-решения)
12. [Планы по развитию](#планы-по-развитию)
13. [Лицензия](#лицензия)

---

## 📖 
## Описание проекта

**Joke Kafka Consumer** — это Spring Boot микросервис, который выполняет одну задачу:

- **Подписывается** на топик Kafka `jokes-topic`.
- **Получает** сообщения с шутками в формате JSON.
- **Проверяет** наличие дубликатов по полю `id`.
- **Сохраняет** уникальные сообщения в PostgreSQL.
- **Сохраняет** метаданные Kafka (partition, offset, timestamp) для каждого сообщения.
- **Предоставляет REST API** для мониторинга и просмотра сохранённых данных.

Проект демонстрирует **production-ready** подход к разработке Kafka Consumer с ручным управлением offset, дедупликацией и обработкой ошибок.

---

## 🧱 
## Архитектура и компоненты

### Диаграмма компонентов

```mermaid
graph TD
    A[Kafka Producer<br/>(внешняя система)] -->|отправляет JokeMessage| B[Kafka Topic<br/>jokes-topic]
    B -->|читает| C[JokeKafkaConsumerService<br/>@KafkaListener]
    C -->|десериализует| D[JsonDeserializer]
    C -->|проверка дубликата| E[JokeRepository.existsByJokeId]
    C -->|сохраняет| F[JokeRepository.save]
    F -->|JPA| G[(PostgreSQL)]
    
    H[REST Client] -->|GET /api/jokes| I[JokeController]
    H -->|GET /api/jokes/stats| I
    
    I -->|запросы| E
    I -->|запросы| F
    
    style C fill:#4CAF50,color:#fff
    style I fill:#2196F3,color:#fff
```
### Диаграмма последовательности (обработка сообщения)

<img width="4240" height="4666" alt="deepseek_mermaid_20260908_4af382" src="https://github.com/user-attachments/assets/4fd937c4-511e-4050-8953-08e89236f966" />

### ER-диаграмма (таблица jokes)
<img width="1941" height="1463" alt="deepseek_mermaid_20260908_ef5386" src="https://github.com/user-attachments/assets/c0e5b4fd-1cac-48ae-ac72-b5cc4db00794" />

## 🔧 
## Технологический стек

|Компонент	|Технология|
|--------------|--------------------|
|Фреймворк|	Spring Boot 3.2.x|
|Обмен сообщениями	|Apache Kafka (Spring Kafka)|
|База данных|	PostgreSQL 15+|
|ORM	|Spring Data JPA (Hibernate 6)|
|Сборка|Maven|
|Язык	|Java 17+|
|Десериализация	|Jackson (JsonDeserializer)|
|Ломбок|	Project Lombok|
|Мониторинг|	Spring Boot Actuator|
|Логирование	|SLF4J + Logback|

## 📂 
## Структура проекта
```bach
joke-kafka-consumer/
├── src/
│   └── main/
│       ├── java/
│       │   └── com/example/jokekafkaconsumer/
│       │       ├── JokeKafkaConsumerApplication.java  # Точка входа с @EnableKafka
│       │       ├── config/
│       │       │   └── KafkaConsumerConfig.java       # Конфигурация Kafka Consumer
│       │       ├── controller/
│       │       │   └── JokeController.java            # REST API для мониторинга
│       │       ├── dto/
│       │       │   └── JokeMessage.java               # DTO для десериализации из Kafka
│       │       ├── model/
│       │       │   └── JokeEntity.java                # JPA-сущность
│       │       ├── repository/
│       │       │   └── JokeRepository.java            # JPA-репозиторий
│       │       └── service/
│       │           └── JokeKafkaConsumerService.java  # Основной Kafka Consumer
│       └── resources/
│           └── application.properties                 # Конфигурация приложения
├── pom.xml
└── docker-compose.yml (опционально)
```

## 🚀 
## Установка и запуск
### Предварительные требования

    * JDK 17 или новее

    * Apache Kafka (локально или через Docker)

    * PostgreSQL 15+ (локально или через Docker)

    * Maven (или использовать встроенный mvnw)

    * Docker и Docker Compose (рекомендуется)

 ### Настройка окружения (Docker Compose)

#### Создайте docker-compose.yml в корне проекта:   
```bach
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    container_name: zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:latest
    container_name: kafka
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
    ports:
      - "9092:9092"

  postgres:
    image: postgres:15
    container_name: postgres
    environment:
      POSTGRES_DB: jokes_db
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: 12345
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

volumes:
  postgres_data:
```
#### Запустите инфраструктуру:
```bach
docker-compose up -d
```
#### Проверьте, что всё работает:
```bach
docker ps
```
#### Настройка приложения

#### Файл src/main/resources/application.properties:
```bach
#properties
# ===== Сервер =====
server.port=8081

# ===== KAFKA CONSUMER =====
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=joke-consumer-group
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.consumer.enable-auto-commit=false          # Ручной ack
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer
spring.kafka.consumer.properties.spring.json.trusted.packages=*
spring.kafka.consumer.properties.spring.json.value.default.type=com.example.jokekafkaconsumer.dto.JokeMessage
spring.kafka.listener.ack-mode=manual                   # Ручное подтверждение
spring.kafka.listener.concurrency=1
spring.kafka.listener.poll-timeout=3000

# ===== POSTGRESQL =====
spring.datasource.url=jdbc:postgresql://localhost:5432/jokes_db
spring.datasource.username=postgres
spring.datasource.password=12345
spring.datasource.driver-class-name=org.postgresql.Driver
spring.datasource.hikari.maximum-pool-size=5

# ===== JPA =====
spring.jpa.hibernate.ddl-auto=update
spring.jpa.show-sql=true
spring.jpa.properties.hibernate.format_sql=true

# ===== ЛОГИРОВАНИЕ =====
logging.level.com.example.jokekafkaconsumer=DEBUG
logging.level.org.springframework.kafka=INFO
logging.level.org.apache.kafka.clients.consumer=INFO
logging.level.org.hibernate.SQL=DEBUG
logging.level.org.hibernate.orm.jdbc.bind=TRACE

# ===== ACTUATOR =====
management.endpoints.web.exposure.include=health,info,metrics

```
### Сборка и запуск
```bach
# Сборка проекта
./mvnw clean package

# Запуск приложения
./mvnw spring-boot:run
```
* Приложение будет доступно по адресу: http://localhost:8081

## ⚙️ 
## Конфигурация
### Kafka Consumer
|Свойство	|Значение	|Описание|
|-----------------|---------------|---------------------|
|spring.kafka.bootstrap-servers|	localhost:9092|	Адрес брокера Kafka|
|spring.kafka.consumer.group-id	|joke-consumer-group|	ID группы потребителей\
|spring.kafka.consumer.auto-offset-reset|	earliest	|Начинать чтение с самого начала|
|spring.kafka.consumer.enable-auto-commit	|false|	Важно! Отключаем авто-коммит|
|spring.kafka.listener.ack-mode	manual	|Ручное| подтверждение offset|
|spring.kafka.listener.concurrency	|1	|Количество потоков-слушателей|


### База данных

|Свойство|	Значение|	Описание|
|---------------------|----------------------------|
spring.datasource.url|	jdbc:postgresql://localhost:5432/jokes_db|	URL подключения|
spring.datasource.username	|postgres|	Имя пользователя|
spring.datasource.password	|12345|	Пароль|
spring.jpa.hibernate.ddl-auto|	update|	Автосоздание/обновление схемы|

### Логирование
|Логгер	|Уровень	|Описание|
|--------------------------|---------------------------------|
|com.example.jokekafkaconsumer	|DEBUG|	Логи приложения (получение, сохранение)|
|org.springframework.kafka	INFO|Логи| Kafka (подключение, получение)|
|org.hibernate.SQL	|DEBUG|	Вывод SQL-запросов|
|org.hibernate.orm.jdbc.bind|	TRACE|	Параметры запросов|

## 🌐 
## REST API (для мониторинга)
### REST API предоставляет доступ к сохранённым данным и статистике работы consumer.

#### Получить все шутки (с пагинацией)
```bach
GET /api/jokes?page=0&size=20
```
#### Параметры:
* page - номер страницы (по умолчанию 0)

* size - размер страницы (по умолчанию 20)

#### Пример ответа:
```bach
{
  "content": [
    {
      "id": 1,
      "jokeId": 42,
      "type": "programming",
      "setup": "Why do programmers prefer dark mode?",
      "punchline": "Because light attracts bugs.",
      "createdAt": "2026-09-08T10:00:00",
      "kafkaOffset": 15,
      "kafkaPartition": 0,
      "kafkaTimestamp": "2026-09-08T10:00:00"
    }
  ],
  "pageable": {
    "pageNumber": 0,
    "pageSize": 20
  },
  "totalElements": 100,
  "totalPages": 5
}
```
#### Получить последние 10 шуток
```bach
GET /api/jokes/latest
```
#### Пример ответа:
```bach
{
  "totalInDB": 157,
  "byType": {
    "programming": 89,
    "general": 45,
    "knock-knock": 23
  },
  "consumer": {
    "processed": 157,
    "duplicates": 12,
    "errors": 3,
    "lastOffset": 171
  }
}
```
#### Что означают поля:
* processed - количество успешно обработанных и сохранённых сообщений

* duplicates - количество пропущенных дубликатов

* errors - количество ошибок при обработке

* lastOffset - последний обработанный offset


#### Поиск по ключевому слову

```bach
GET /api/jokes/search?keyword=programmer
```
##🔍 
##Особенности реализации
### Дедупликация сообщений
```bach
// Проверка перед сохранением
if (jokeRepository.existsByJokeId(joke.getId())) {
    log.warn("⚠️ Дубликат ID {} – пропускаем", joke.getId());
    totalDuplicates.incrementAndGet();
    acknowledgment.acknowledge();
    return;
}
```
* Уникальность гарантируется на уровне БД через @UniqueConstraint(columnNames = {"joke_id"})

* Дубликаты логируются, но не сохраняются

* Offset подтверждается, чтобы не блокировать очередь

### Ручное подтверждение offset (Manual Ack)
```bach
@KafkaListener(topics = "jokes-topic")
public void consumeJoke(..., Acknowledgment acknowledgment) {
    try {
        // ... обработка
        acknowledgment.acknowledge(); // Явное подтверждение
    } catch (Exception e) {
        // Ошибка логируется, но offset подтверждается
        acknowledgment.acknowledge();
    }
}
```
#### Преимущества:

    * Полный контроль над подтверждением

    * Предотвращение потери сообщений при ошибках

    * Возможность повторной обработки


### Обработка ошибок 
 ```bach
@Bean
public ConcurrentKafkaListenerContainerFactory<...> kafkaListenerContainerFactory() {
    factory.setCommonErrorHandler(
        new DefaultErrorHandler(new FixedBackOff(1000L, 3L))
    );
    return factory;
}
```
* Настроен DefaultErrorHandler с 3 попытками повторной обработки

* Интервал между попытками - 1 секунда

* При ошибке offset подтверждается (чтобы не блокировать очередь)

* Все ошибки логируются с полным стек-трейсом

* Счётчик ошибок доступен через /api/jokes/stats

### Транзакционность
```bach
@KafkaListener(...)
@Transactional // Гарантирует атомарность
public void consumeJoke(...) {
    jokeRepository.save(entity);
}
```
* Использование @Transactional обеспечивает атомарность операций с БД

* При ошибке транзакция откатывается

* Сообщение не теряется (offset не подтверждён)

### Сохранение метаданных Kafka
```bach
entity.setKafkaPartition(partition);
entity.setKafkaOffset(offset);
entity.setKafkaTimestamp(
    LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault())
);
```
### Каждое сохранённое сообщение содержит:

    * kafka_partition - номер партиции

    * kafka_offset - offset сообщения

    * kafka_timestamp - временная метка из Kafka

### Это позволяет:

    * Отслеживать происхождение данных

    * Анализировать задержки

    * Восстанавливать данные при необходимости

## 🧪 
## Тестирование
#### Интеграционное тестирование с Kafka

#### Для тестирования можно использовать Embedded Kafka или Testcontainers:  
```bach
<!-- В pom.xml -->
<dependency>
    <groupId>org.springframework.kafka</groupId>
    <artifactId>spring-kafka-test</artifactId>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>kafka</artifactId>
    <scope>test</scope>
</dependency>
```
### Ручное тестирование

#### 1. Проверьте, что топик существует:
```bach
docker exec -it kafka kafka-topics \
  --list \
  --bootstrap-server localhost:9092
```
#### Если топика нет, создайте:
```bach
docker exec -it kafka kafka-topics \
  --create \
  --topic jokes-topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```
#### 2. Отправьте тестовые сообщения через консольного продюсера:
```bach
docker exec -it kafka kafka-console-producer \
  --broker-list localhost:9092 \
  --topic jokes-topic
```
### Отправьте сообщения (по одному на строку):
```bach
#json
{"id": 1, "type": "programming", "setup": "Why do Java developers wear glasses?", "punchline": "Because they can't C#!"}
{"id": 2, "type": "general", "setup": "Why did the programmer go broke?", "punchline": "Because he used up all his cache."}
{"id": 1, "type": "programming", "setup": "Duplicate message", "punchline": "Should be skipped"}
```
### 3. Проверьте, что сообщения сохранились:






 
































