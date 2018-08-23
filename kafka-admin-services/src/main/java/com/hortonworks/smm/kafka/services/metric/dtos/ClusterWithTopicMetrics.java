package com.hortonworks.smm.kafka.services.metric.dtos;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ClusterWithTopicMetrics {

    @JsonProperty
    private Long totalBytesIn;

    @JsonProperty
    private Long totalBytesOut;

    @JsonProperty
    private Double producedPerSec;

    @JsonProperty
    private Double fetchedPerSec;

    @JsonProperty
    private Long inSyncReplicas;

    @JsonProperty
    private Long outOfSyncReplicas;

    @JsonProperty
    private Long underReplicatedPartitions;

    @JsonProperty
    private Long offlinePartitions;

    @JsonProperty
    private Collection<AggrTopicMetrics> aggrTopicMetricsCollection;


    private ClusterWithTopicMetrics() {

    }

    public ClusterWithTopicMetrics(Long totalBytesIn, Long totalBytesOut, Double producedPerSec, Double fetchedPerSec,
                                   Long inSyncReplicas, Long outOfSyncReplicas, Long underReplicatedPartitions,
                                   Long offlinePartitions, Collection<AggrTopicMetrics> aggrTopicMetricsCollection) {
        this.totalBytesIn = totalBytesIn;
        this.totalBytesOut = totalBytesOut;
        this.producedPerSec = producedPerSec;
        this.fetchedPerSec = fetchedPerSec;
        this.inSyncReplicas = inSyncReplicas;
        this.outOfSyncReplicas = outOfSyncReplicas;
        this.underReplicatedPartitions = underReplicatedPartitions;
        this.offlinePartitions = offlinePartitions;
        this.aggrTopicMetricsCollection = aggrTopicMetricsCollection;
    }


    public Long totalBytesIn() {
        return totalBytesIn;
    }

    public Long totalBytesOut() {
        return totalBytesOut;
    }

    public Double producedPerSec() {
        return producedPerSec;
    }

    public Double fetchedPerSec() {
        return fetchedPerSec;
    }

    public Long inSyncReplicas() {
        return inSyncReplicas;
    }

    public Long outOfSyncReplicas() {
        return outOfSyncReplicas;
    }

    public Long underReplicatedPartitions() {
        return underReplicatedPartitions;
    }

    public Long offlinePartitions() {
        return offlinePartitions;
    }

    public Collection<AggrTopicMetrics> aggrTopicMetricsCollection() {
        return aggrTopicMetricsCollection;
    }

    @Override
    public String toString() {
        return "ClusterWithTopicMetrics{" +
                "totalBytesIn=" + totalBytesIn +
                ", totalBytesOut=" + totalBytesOut +
                ", producedPerSec=" + producedPerSec +
                ", fetchedPerSec=" + fetchedPerSec +
                ", inSyncReplicas=" + inSyncReplicas +
                ", outOfSyncReplicas=" + outOfSyncReplicas +
                ", underReplicatedPartitions=" + underReplicatedPartitions +
                ", offlinePartitions=" + offlinePartitions +
                " aggrTopicMetricsCollection=" + aggrTopicMetricsCollection +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterWithTopicMetrics that = (ClusterWithTopicMetrics) o;
        return Objects.equals(totalBytesIn, that.totalBytesIn) &&
                Objects.equals(totalBytesOut, that.totalBytesOut) &&
                Objects.equals(producedPerSec, that.producedPerSec) &&
                Objects.equals(fetchedPerSec, that.fetchedPerSec) &&
                Objects.equals(inSyncReplicas, that.inSyncReplicas) &&
                Objects.equals(outOfSyncReplicas, that.outOfSyncReplicas) &&
                Objects.equals(underReplicatedPartitions, that.underReplicatedPartitions) &&
                Objects.equals(offlinePartitions, that.offlinePartitions) &&
                Objects.equals(aggrTopicMetricsCollection, that.aggrTopicMetricsCollection);

    }

    @Override
    public int hashCode() {
        return Objects.hash(totalBytesIn, totalBytesOut, producedPerSec, fetchedPerSec, inSyncReplicas,
                outOfSyncReplicas, underReplicatedPartitions, offlinePartitions, aggrTopicMetricsCollection);
    }
}
