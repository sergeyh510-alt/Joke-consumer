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


## 📚 Оглавление

* [О проекте](#О-проекте)

* [Архитектура и поток данных](#Архитектура-и-поток-данных)

* [Основные возможности](#Основные-возможности)

* [Технологический стек](#Технологический-стек)

* [Начало работы](#Начало-работы)

* [Предварительные требования](#Предварительные-требования)

* [Установка и запуск](#Установка-и-запуск)

* [Настройка](#Настройка)
* [Тестирование](#Тестирование)

* [Лицензия](#Лицензия)

## 🎯 
## О проекте

Проект Joke Consumer входит в состав системы обработки шуток. Он подписывается на указанный топик Kafka, получает сообщения с шутками (в формате JSON), 
десериализует их,выполняет необходимую валидацию и сохраняет в реляционную базу данных для дальнейшего использования.

Этот сервис является примером реализации паттерна «Competing Consumers» и может быть легко масштабирован горизонтально для увеличения пропускной способности.


## 📊 
## Архитектура и поток данных

<img width="1726" height="358" alt="image" src="https://github.com/user-attachments/assets/0659f510-a4a1-4c6c-b9d8-b620ff536df7" />




### Поток данных:

   * Производитель отправляет сообщение с шуткой в топик Kafka.

   * Joke Consumer слушает топик и получает новое сообщение.

   * Сервис десериализует JSON в объект доменной модели.

   * Выполняется проверка данных (например, на наличие текста и типа шутки).

   * Валидные шутки сохраняются в базе данных.

   * В случае ошибок, сообщение может быть отправлено в DLQ (Dead Letter Queue) или залогировано.

---

## ✨ 
## Основные возможности

 *   Чтение из Kafka: Подключение к кластеру Kafka и чтение сообщений из указанного топика.

 *   Сохранение в БД: Поддержка JPA для сохранения полученных данных в реляционную базу (PostgreSQL/MySQL/H2).

 *   Обработка ошибок: Базовые механизмы повторной обработки и логирования ошибок.

 *   Конфигурируемость: Гибкие настройки через application.yml (адреса брокеров, топик, группа потребителей).

 *   Простота запуска: Сборка с помощью Maven и запуск как самостоятельного Spring Boot приложения.

## 🛠 
## Технологический стек

|Компонент	|Технология|	Версия|
|-------------------|---------------------|
|Язык|	Java	17+
|Фреймворк	|Spring Boot	|3.4.0|
|Клиент Kafka|	Spring Kafka|	3.3.0|
|Работа с БД	|Spring Data JPA	|-|
|Сборка	|Maven|	3.9.0+
|Брокер сообщений|	Apache Kafka|	3.8.0|
|База данных|	PostgreSQL / MySQL / H2 (по умолчанию H2 для разработки)|	-|
|Документирование|	Mermaid для диаграмм|	-|

## 🚀 
## Начало работы
### Предварительные требования

#### Для успешного запуска проекта убедитесь, что у вас установлены:

*    Java Development Kit (JDK) версии 17 или выше.

*    Apache Maven версии 3.9.0+.

*    Apache Kafka кластер (локальный или удаленный) с созданным топиком jokes.

*    Система управления базами данных (PostgreSQL/MySQL) или используйте встроенную H2 для тестов.

### Установка и запуск

#### 1. Клонируйте репозиторий:

```bach
git clone https://github.com/sergeyh510-alt/Joke-consumer.git
cd Joke-consume
```
2. Настройте подключение к Kafka и БД в файле src/main/resources/application.yml. Пример базовой конфигурации:
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
#### 3.Соберите проект с помощью Maven:
```bach
./mvnw clean package
```
#### 4.Запустите приложение:
```bach
./mvnw spring-boot:run
```
#### Или запустите собранный JAR-файл:
```bach
java -jar target/joke-consumer-0.0.1-SNAPSHOT.jar
```
## ⚙️ 
## Настройка
#### Ключевые параметры конфигурации вынесены в application.yml. Основные из них:

|Параметр	|Описание|	Значение по умолчанию|
|------------------------------------------|--------------------------------|
spring.kafka.bootstrap-servers|	Адреса брокеров Kafka|	localhost:9092|
spring.kafka.consumer.group-id	|ID группы потребителей	|joke-consumer-group|
spring.kafka.consumer.auto-offset-reset	|Стратегия сброса смещения (earliest/latest)|	earliest|
spring.datasource.url	|URL для подключения к БД|	jdbc:h2:mem:jokedb
joke.topic.name (добавьте сами)|	Имя топика для чтения шуток|	jokes|


#### Для продакшена рекомендуется переопределить параметры через переменные окружения или внешний файл конфигурации.


## 🧪 
## Тестирование
#### Проект включает базовые модульные тесты для проверки:

*    Десериализации сообщений.

*    Логики сохранения в репозиторий.

*    Обработки ошибок.

#### Для запуска тестов выполните:
```bach
./mvnw test
```
#### Для более глубокого тестирования с реальной Kafka можно использовать Testcontainers.

## 📄 
##Лицензия
Проект распространяется под лицензией MIT. Подробности смотрите в файле LICENSE.

#### Joke Consumer — простой, но рабочий пример микросервиса-потребителя Kafka. 
#### Надеюсь, этот проект будет полезен для изучения или использования в качестве основы для ваших собственных решений 😄

### 📞 
### Контакты
* Contact Sergey Chekryzhov
* Email sergeyh510@gmail.com
* GitHub sergeyh510-alt
* Project Joke Kafka Consumer
* LinkedIn: www.linkedin.com/in/sergey-chekryzhov-a38778217
* Telegram: @SergeyChekryzhov
