package ru.practicum.analyzer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.practicum.analyzer.model.Action;

import java.util.List;

public interface ActionRepository extends JpaRepository<Action, Long> {

    @Query("""
            select distinct a
            from ScenarioAction sa
            join sa.action a
            where sa.sensor.id = :sensorId
            """)
    List<Action> findBySensorId(@Param("sensorId") String sensorId);
}