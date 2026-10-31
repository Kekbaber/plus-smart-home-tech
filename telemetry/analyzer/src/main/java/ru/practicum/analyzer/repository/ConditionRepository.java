package ru.practicum.analyzer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.practicum.analyzer.model.Condition;

import java.util.List;

public interface ConditionRepository extends JpaRepository<Condition, Long> {

    @Query("""
            select distinct c
            from ScenarioCondition sc
            join sc.condition c
            where sc.sensor.id = :sensorId
            """)
    List<Condition> findBySensorId(@Param("sensorId") String sensorId);
}