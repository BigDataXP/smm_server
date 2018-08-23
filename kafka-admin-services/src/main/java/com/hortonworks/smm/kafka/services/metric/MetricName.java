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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class MetricName {

    private final String groupName;
    private final String typeName;
    private final String name;
    private final String attribute;
    private final List<String> tags;

    private MetricName(String groupName, String typeName, String name, String attribute, List<String> tags) {
        this.groupName = groupName;
        this.typeName = typeName;
        this.name = name;
        this.attribute = attribute;

        // NOTE: Kafka broker sorts the tags in alphabetical order while emitting the metrics.
        // So, the tags are order as alphabetical.
        List<String> metricTags = new ArrayList<>(tags);
        Collections.sort(metricTags);
        this.tags = Collections.unmodifiableList(metricTags);
    }

    public String getGroupName() {
        return groupName;
    }

    public String getTypeName() {
        return typeName;
    }

    public String getName() {
        return name;
    }

    public String getAttribute() {
        return attribute;
    }

    public List<String> getTags() {
        return tags;
    }

    @Override
    public String toString() {
        return "MetricName{" +
                "groupName='" + groupName + '\'' +
                ", typeName='" + typeName + '\'' +
                ", name='" + name + '\'' +
                ", attribute='" + attribute + '\'' +
                ", tags=" + tags +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetricName that = (MetricName) o;
        return Objects.equals(groupName, that.groupName) &&
                Objects.equals(typeName, that.typeName) &&
                Objects.equals(name, that.name) &&
                Objects.equals(attribute, that.attribute) &&
                Objects.equals(tags, that.tags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupName, typeName, name, attribute, tags);
    }

    public static MetricName from(String groupName,
                                  String typeName,
                                  String name,
                                  String attribute,
                                  List<String> tags) {
        return new Builder(groupName).typeName(typeName).name(name).attribute(attribute).tags(tags).build();
    }

    public static Builder newBuilder(String groupName) {
        return new Builder(groupName);
    }

    public static class Builder {
        private static final Map<MetricName, MetricName> namePool = new ConcurrentHashMap<>();

        private String groupName;
        private String typeName;
        private String name;
        private String attribute = AbstractMetricDescriptorSupplier.EMPTY_ATTR;
        private List<String> tags;

        private Builder(String groupName) {
            this.groupName = groupName;
        }

        public Builder typeName(String typeName) {
            this.typeName = typeName;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder attribute(String attribute) {
            this.attribute = attribute;
            return this;
        }

        public Builder tags(List<String> tags) {
            this.tags = tags;
            return this;
        }

        public MetricName build() {
            MetricName metricName = new MetricName(groupName, typeName, name, attribute, tags);
            // This is done to reduce the memory foot print of MetricName object
            return namePool.computeIfAbsent(metricName, x -> x);
        }
    }

}
