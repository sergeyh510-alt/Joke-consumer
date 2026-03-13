package com.example.jokekafkaconsumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class JokeKafkaConsumerApplication {
	public static void main(String[] args) {
		SpringApplication.run(JokeKafkaConsumerApplication.class, args);
	}
}