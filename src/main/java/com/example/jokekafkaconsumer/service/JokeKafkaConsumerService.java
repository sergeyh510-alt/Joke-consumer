package com.example.jokekafkaconsumer.service;

import com.example.jokekafkaconsumer.dto.JokeMessage;
import com.example.jokekafkaconsumer.model.JokeEntity;
import com.example.jokekafkaconsumer.repository.JokeRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicLong;

@Service
@Slf4j
public class JokeKafkaConsumerService {

    private final JokeRepository jokeRepository;
    private final ObjectMapper objectMapper;

    private final AtomicLong totalProcessed = new AtomicLong(0);
    private final AtomicLong totalDuplicates = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);
    private final AtomicLong lastOffset = new AtomicLong(0);

    @Autowired
    public JokeKafkaConsumerService(JokeRepository jokeRepository, ObjectMapper objectMapper) {
        this.jokeRepository = jokeRepository;
        this.objectMapper = objectMapper;
        log.info("✅ JokeKafkaConsumerService инициализирован");
    }

    @KafkaListener(topics = "jokes-topic", groupId = "${spring.kafka.consumer.group-id}")
    @Transactional
    public void consumeJoke(
            @Payload(required = false) JokeMessage joke,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
            Acknowledgment acknowledgment) {

        log.info("=".repeat(100));
        log.info("📥 ПОЛУЧЕНО СООБЩЕНИЕ. Offset: {}", offset);
        boolean processed = false;

        try {
            if (joke == null) {
                log.error("❌ Сообщение null");
                totalErrors.incrementAndGet();
                acknowledgment.acknowledge();
                processed = true;
                return;
            }

            log.info("   ID: {}, Type: {}", joke.getId(), joke.getType());
            log.info("   Setup: {}", joke.getSetup());

            if (jokeRepository.existsByJokeId(joke.getId())) {
                log.warn("⚠️ Дубликат ID {} – пропускаем", joke.getId());
                totalDuplicates.incrementAndGet();
                acknowledgment.acknowledge();
                processed = true;
                return;
            }

            JokeEntity entity = new JokeEntity();
            entity.setJokeId(joke.getId());
            entity.setType(joke.getType());
            entity.setSetup(joke.getSetup());
            entity.setPunchline(joke.getPunchline());
            entity.setKafkaPartition(partition);
            entity.setKafkaOffset(offset);
            entity.setKafkaTimestamp(LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(timestamp), ZoneId.systemDefault()));

            JokeEntity saved = jokeRepository.save(entity);
            log.info("✅ СОХРАНЕНО в БД! ID в БД: {}", saved.getId());
            totalProcessed.incrementAndGet();
            lastOffset.set(offset);
            acknowledgment.acknowledge();
            processed = true;

        } catch (Exception e) {
            log.error("❌ ОШИБКА при обработке offset {}:", offset);
            log.error("   Тип ошибки: {}", e.getClass().getName());
            log.error("   Сообщение: {}", e.getMessage());
            log.error("   Стек вызовов:", e);
            // Дублируем в System.err, если логирование не работает
            System.err.println("ERROR in consumer for offset " + offset);
            e.printStackTrace();

            totalErrors.incrementAndGet();

            if (!processed) {
                acknowledgment.acknowledge();
                log.warn("⚠️ Сообщение с offset {} пропущено из-за ошибки", offset);
            }
        }
    }
    public Long getTotalProcessed() {
        return totalProcessed.get();
    }

    public Long getTotalDuplicates() {
        return totalDuplicates.get();
    }

    public Long getTotalErrors() {
        return totalErrors.get();
    }

    public Long getLastOffset() {
        return lastOffset.get();
    }
}