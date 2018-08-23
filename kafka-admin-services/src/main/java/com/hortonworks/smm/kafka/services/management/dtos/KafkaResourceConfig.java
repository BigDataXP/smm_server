package com.hortonworks.smm.kafka.services.management.dtos;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class KafkaResourceConfig {

    @JsonProperty
    private String resourceType;

    @JsonProperty
    private String resourceName;

    @JsonProperty
    private List<KafkaResourceConfigEntry> resourceConfigs;

    private KafkaResourceConfig() {
    }

    public KafkaResourceConfig(ConfigResource.Type resourceType,
                               String resourceName,
                               List<KafkaResourceConfigEntry> resourceConfigs) {
        Objects.requireNonNull(resourceType, "resourceType can not be null");
        Objects.requireNonNull(resourceName, "resourceName can not be null");
        Objects.requireNonNull(resourceConfigs, "resourceConfigs can not be null");

        this.resourceType = resourceType.name();
        this.resourceName = resourceName;
        this.resourceConfigs = resourceConfigs;
    }

    public String resourceType() {
        return resourceType;
    }

    public String name() {
        return resourceName;
    }

    public Collection<KafkaResourceConfigEntry> resourceConfigs() {
        return resourceConfigs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KafkaResourceConfig that = (KafkaResourceConfig) o;
        return Objects.equals(resourceType, that.resourceType) &&
                Objects.equals(resourceName, that.resourceName) &&
                Objects.equals(resourceConfigs, that.resourceConfigs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceType, resourceName, resourceConfigs);
    }

    @Override
    public String toString() {
        return "KafkaResourceConfig{" +
                "resourceType=" + resourceType +
                ", resourceName='" + resourceName + '\'' +
                ", resourceConfigs=" + resourceConfigs +
                '}';
    }

}
