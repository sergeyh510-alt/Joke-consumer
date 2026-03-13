package com.example.jokekafkaconsumer.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;

@Entity
@Table(name = "jokes",
        uniqueConstraints = {@UniqueConstraint(columnNames = {"joke_id"})},
        indexes = {
                @Index(name = "idx_joke_id", columnList = "joke_id"),
                @Index(name = "idx_created_at", columnList = "created_at")
        })
@Data
@NoArgsConstructor
@AllArgsConstructor
public class JokeEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "joke_id", nullable = false, unique = true)
    private Integer jokeId;

    @Column(nullable = false, length = 5000)
    private String type;

    @Column(nullable = false, length = 5000)
    private String setup;

    @Column(nullable = false, length = 5000)
    private String punchline;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "kafka_offset")
    private Long kafkaOffset;

    @Column(name = "kafka_partition")
    private Integer kafkaPartition;

    @Column(name = "kafka_timestamp")
    private LocalDateTime kafkaTimestamp;
}