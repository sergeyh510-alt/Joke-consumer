package com.example.jokekafkaconsumer.controller;

import com.example.jokekafkaconsumer.model.JokeEntity;
import com.example.jokekafkaconsumer.repository.JokeRepository;
import com.example.jokekafkaconsumer.service.JokeKafkaConsumerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/jokes")
@RequiredArgsConstructor
@Slf4j
@Service
public class JokeController {

    private final JokeRepository jokeRepository;
    private final JokeKafkaConsumerService consumerService;

    @GetMapping
    public ResponseEntity<Page<JokeEntity>> getAllJokes(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {

        Page<JokeEntity> jokes = jokeRepository.findAll(
                PageRequest.of(page, size, Sort.by("createdAt").descending()));
        return ResponseEntity.ok(jokes);
    }

    @GetMapping("/latest")
    public ResponseEntity<List<JokeEntity>> getLatestJokes() {
        List<JokeEntity> jokes = jokeRepository.findAll(
                PageRequest.of(0, 10, Sort.by("createdAt").descending())
        ).getContent();
        return ResponseEntity.ok(jokes);
    }

    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getStats() {
        Map<String, Object> stats = new HashMap<>();

        // Статистика из БД
        stats.put("totalInDB", jokeRepository.count());

        // Статистика по типам (исправлено!)
        List<Object[]> typeStats = jokeRepository.countByTypeGrouped();
        Map<String, Long> typeMap = new HashMap<>();
        if (typeStats != null) {
            typeStats.forEach(arr -> typeMap.put((String) arr[0], (Long) arr[1]));
        }
        stats.put("byType", typeMap);

        // Статистика от consumer
        Map<String, Object> consumerStats = new HashMap<>();
        consumerStats.put("processed", consumerService.getTotalProcessed());
        consumerStats.put("duplicates", consumerService.getTotalDuplicates());
        consumerStats.put("errors", consumerService.getTotalErrors());
        consumerStats.put("lastOffset", consumerService.getLastOffset());
        stats.put("consumer", consumerStats);

        return ResponseEntity.ok(stats);
    }

    @GetMapping("/debug")
    public ResponseEntity<Map<String, Object>> getDebugInfo() {
        Map<String, Object> debug = new HashMap<>();
        debug.put("totalInDB", jokeRepository.count());
        debug.put("consumerProcessed", consumerService.getTotalProcessed());
        debug.put("consumerDuplicates", consumerService.getTotalDuplicates());
        debug.put("consumerErrors", consumerService.getTotalErrors());
        debug.put("lastOffset", consumerService.getLastOffset());
        return ResponseEntity.ok(debug);
    }

    @GetMapping("/search")
    public ResponseEntity<List<JokeEntity>> searchJokes(@RequestParam String keyword) {
        List<JokeEntity> jokes = jokeRepository.searchBySetup(keyword);
        return ResponseEntity.ok(jokes);
    }

    @DeleteMapping("/clear")
    public ResponseEntity<String> clearAllJokes() {
        jokeRepository.deleteAll();
        log.warn(" Все шутки удалены из БД");
        return ResponseEntity.ok("Все данные очищены");
    }

    @GetMapping("/test-save")
    public ResponseEntity<String> testSave() {
        try {
            JokeEntity test = new JokeEntity();
            test.setJokeId(999999);
            test.setType("test");
            test.setSetup("Тестовая запись " + LocalDateTime.now());
            test.setPunchline("Проверка работы JPA");

            JokeEntity saved = jokeRepository.save(test);
            return ResponseEntity.ok("Сохранено! ID=" + saved.getId());
        } catch (Exception e) {
            return ResponseEntity.status(500).body(" Ошибка: " + e.getMessage());
        }
    }
}