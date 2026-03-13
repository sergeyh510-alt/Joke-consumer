package com.example.jokekafkaconsumer.repository;

import com.example.jokekafkaconsumer.model.JokeEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface JokeRepository extends JpaRepository<JokeEntity, Long> {

    Optional<JokeEntity> findByJokeId(Integer jokeId);

    boolean existsByJokeId(Integer jokeId);

    long countByType(String type);

    @Query("SELECT j.type, COUNT(j) FROM JokeEntity j GROUP BY j.type")
    List<Object[]> countByTypeGrouped();

    @Query("SELECT j FROM JokeEntity j WHERE LOWER(j.setup) LIKE LOWER(CONCAT('%', :keyword, '%'))")
    List<JokeEntity> searchBySetup(@Param("keyword") String keyword);
}

