/*
 *   HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 *   (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 *   This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 *   Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 *   to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 *   properly licensed third party, you do not have any rights to this code.
 *
 *   If this code is provided to you under the terms of the AGPLv3:
 *   (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 *   (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *     LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 *   (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *     FROM OR RELATED TO THE CODE; AND
 *   (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *     DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *     DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *     OR LOSS OR CORRUPTION OF DATA.
 */

package com.hortonworks.smm.kafka.services.metric.dtos;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ClusterWithBrokerMetrics {

    @JsonProperty
    private Long totalBytesIn;

    @JsonProperty
    private Long totalBytesOut;

    @JsonProperty
    private Double producedPerSec;

    @JsonProperty
    private Double fetchedPerSec;

    @JsonProperty
    private Integer activeControllers;

    @JsonProperty
    private Double uncleanLeaderElectionsPerSec;

    @JsonProperty
    private Double requestPoolUsage;

    @JsonProperty
    private Collection<AggrBrokerMetrics> aggrBrokerMetricsCollection;


    private ClusterWithBrokerMetrics() {

    }

    public ClusterWithBrokerMetrics(Long totalBytesIn, Long totalBytesOut, Double producedPerSec, Double fetchedPerSec,
                                    Integer activeControllers, Double uncleanLeaderElectionsPerSec, Double requestPoolUsage,
                                    Collection<AggrBrokerMetrics> aggrBrokerMetricsCollection) {
        this.totalBytesIn = totalBytesIn;
        this.totalBytesOut = totalBytesOut;
        this.producedPerSec = producedPerSec;
        this.fetchedPerSec = fetchedPerSec;
        this.activeControllers = activeControllers;
        this.uncleanLeaderElectionsPerSec = uncleanLeaderElectionsPerSec;
        this.requestPoolUsage = requestPoolUsage;
        this.aggrBrokerMetricsCollection = aggrBrokerMetricsCollection;
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

    public Integer activeControllers() {
        return activeControllers;
    }

    public Double uncleanLeaderElectionsPerSec() {
        return uncleanLeaderElectionsPerSec;
    }

    public Double requestPoolUsage() {
        return requestPoolUsage;
    }

    public Collection<AggrBrokerMetrics> aggrBrokerMetricsCollection() {
        return aggrBrokerMetricsCollection;
    }

    @Override
    public String toString() {
        return "ClusterWithBrokerMetrics{" +
                "totalBytesIn=" + totalBytesIn +
                ", totalBytesOut=" + totalBytesOut +
                ", producedPerSec=" + producedPerSec +
                ", fetchedPerSec=" + fetchedPerSec +
                ", activeControllers=" + activeControllers +
                ", uncleanLeaderElectionsPerSec=" + uncleanLeaderElectionsPerSec +
                ", requestPoolUsage=" + requestPoolUsage +
                " aggrBrokerMetricsCollection=" + aggrBrokerMetricsCollection +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterWithBrokerMetrics that = (ClusterWithBrokerMetrics) o;
        return Objects.equals(totalBytesIn, that.totalBytesIn) &&
                Objects.equals(totalBytesOut, that.totalBytesOut) &&
                Objects.equals(producedPerSec, that.producedPerSec) &&
                Objects.equals(fetchedPerSec, that.fetchedPerSec) &&
                Objects.equals(activeControllers, that.activeControllers) &&
                Objects.equals(uncleanLeaderElectionsPerSec, that.uncleanLeaderElectionsPerSec) &&
                Objects.equals(requestPoolUsage, that.requestPoolUsage) &&
                Objects.equals(aggrBrokerMetricsCollection, that.aggrBrokerMetricsCollection);

    }

    @Override
    public int hashCode() {
        return Objects.hash(totalBytesIn, totalBytesOut, producedPerSec, fetchedPerSec, activeControllers,
                uncleanLeaderElectionsPerSec, requestPoolUsage, aggrBrokerMetricsCollection  );
    }
}