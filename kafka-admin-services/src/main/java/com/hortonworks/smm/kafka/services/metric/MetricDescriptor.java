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

package com.hortonworks.smm.kafka.services.metric;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class MetricDescriptor {

    private final MetricName metricName;
    private final Map<String, String> queryTags;
    private final AggregateFunction aggrFunction;
    private final PostProcessFunction postProcessFunction;

    /**
     * Constructor
     * @param metricName           metric name
     * @param queryTags            query tags used in building metric name
     * @param aggrFunction         function to apply while aggregating data across cluster
     * @param postProcessFunction  function to apply for post processing
     */
    private MetricDescriptor(MetricName metricName,
                             Map<String, String> queryTags,
                             AggregateFunction aggrFunction,
                             PostProcessFunction postProcessFunction) {
        this.metricName = metricName;
        this.queryTags = queryTags;
        this.aggrFunction = aggrFunction;
        this.postProcessFunction = postProcessFunction;
    }

    public static MetricDescriptorBuilder newBuilder() {
        return new MetricDescriptorBuilder();
    }

    public MetricName metricName() {
        return metricName;
    }

    public Map<String, String> queryTags() {
        return queryTags;
    }

    public AggregateFunction aggrFunction() {
        return aggrFunction;
    }

    public PostProcessFunction postProcessFunction() {
        return postProcessFunction;
    }

    @Override
    public String toString() {
        return "MetricDescriptor{" +
                "metricName=" + metricName +
                ", queryTags=" + queryTags +
                ", aggrFunction=" + aggrFunction +
                ", postProcessFunction=" + postProcessFunction +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetricDescriptor that = (MetricDescriptor) o;
        return Objects.equals(metricName, that.metricName) &&
                Objects.equals(queryTags, that.queryTags) &&
                Objects.equals(aggrFunction, that.aggrFunction) &&
                Objects.equals(postProcessFunction, that.postProcessFunction);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metricName, queryTags, aggrFunction, postProcessFunction);
    }

    public static class MetricDescriptorBuilder {

        private Map<String, String> queryTags = Collections.emptyMap();
        private AggregateFunction aggrFunction;
        private PostProcessFunction postProcessFunction;

        private MetricDescriptorBuilder() {
        }

        public MetricDescriptorBuilder withQueryTags(Map<String, String> queryTags) {
            this.queryTags = queryTags;
            return this;
        }

        public MetricDescriptorBuilder withAggregationFunction(AggregateFunction aggrFunction) {
            this.aggrFunction = aggrFunction;
            return this;
        }

        public MetricDescriptorBuilder withPostProcessFunction(PostProcessFunction postProcessFunction) {
            this.postProcessFunction = postProcessFunction;
            return this;
        }

        public MetricDescriptor build(MetricName metricName) {
            return new MetricDescriptor(metricName, queryTags, aggrFunction, postProcessFunction);
        }

    }
}
