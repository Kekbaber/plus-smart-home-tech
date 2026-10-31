package ru.practicum.analyzer.model;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.LinkedHashSet;
import java.util.Set;

@Entity
@Table(
        name = "scenarios",
        uniqueConstraints = @UniqueConstraint(columnNames = {"hub_id", "name"})
)
@Getter
@Setter
@NoArgsConstructor
public class Scenario {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "hub_id", nullable = false)
    private String hubId;

    @Column(nullable = false)
    private String name;

    @OneToMany(mappedBy = "scenario", cascade = CascadeType.ALL, orphanRemoval = true)
    private Set<ScenarioCondition> conditions = new LinkedHashSet<>();

    @OneToMany(mappedBy = "scenario", cascade = CascadeType.ALL, orphanRemoval = true)
    private Set<ScenarioAction> actions = new LinkedHashSet<>();

    public void addCondition(Sensor sensor, Condition condition) {
        conditions.add(new ScenarioCondition(this, sensor, condition));
    }

    public void addAction(Sensor sensor, Action action) {
        actions.add(new ScenarioAction(this, sensor, action));
    }
}
