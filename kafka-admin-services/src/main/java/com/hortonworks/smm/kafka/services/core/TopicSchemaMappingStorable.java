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

package com.hortonworks.smm.kafka.services.core;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.storage.PrimaryKey;
import com.hortonworks.registries.storage.annotation.StorableEntity;
import com.hortonworks.registries.storage.catalog.AbstractStorable;

import java.util.Collections;

@StorableEntity
public class TopicSchemaMappingStorable extends AbstractStorable {

    public static final String NAME_SPACE = "topic_schema_mapping";

    public static final String TOPIC = "topic";

    private Long id;

    private String topic;

    private String keySchemaName;

    private String valueSchemaName;

    private Long timestamp;

    public TopicSchemaMappingStorable() {
    }

    public TopicSchemaMappingStorable(String topicName) {
        this.topic = topicName;
    }

    public TopicSchemaMappingStorable(String topicName, String keySchemaName, String valueSchemaName) {
        this.topic = topicName;
        this.keySchemaName = keySchemaName;
        this.valueSchemaName = valueSchemaName;
    }

    @Override
    @JsonIgnore
    public String getNameSpace() {
        return NAME_SPACE;
    }

    @Override
    @JsonIgnore
    public PrimaryKey getPrimaryKey() {
        return new PrimaryKey(Collections.singletonMap(Schema.Field.of(TOPIC, Schema.Type.STRING), topic));
    }

    @Override
    public Long getId() {
        return id;
    }

    @Override
    public void setId(Long id) {
        this.id = id;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getKeySchemaName() {
        return keySchemaName;
    }

    public void setKeySchemaName(String schemaMetadata) {
        this.keySchemaName = schemaMetadata;
    }

    public String getValueSchemaName() {
        return valueSchemaName;
    }

    public void setValueSchemaName(String valueSchemaName) {
        this.valueSchemaName = valueSchemaName;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "TopicSchemaMetadataStorable{" +
                "id=" + id +
                ", topic=" + getTopic() +
                ", keySchemaName='" + getKeySchemaName() + '\'' +
                ", valueSchemaName='" + getValueSchemaName() + '\'' +
                ", timestamp=" + getTimestamp() +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TopicSchemaMappingStorable that = (TopicSchemaMappingStorable) o;

        if (getId() != null ? !getId().equals(that.getId()) : that.getId() != null) return false;
        if (getTopic() != null ? !getTopic().equals(that.getTopic()) : that.getTopic() != null) return false;
        if (getKeySchemaName() != null ? !getKeySchemaName().equals(that.getKeySchemaName()) : that.getKeySchemaName() != null) return false;
        if (getValueSchemaName() != null ? !getValueSchemaName().equals(that.getValueSchemaName()) : that.getValueSchemaName() != null) return false;
        return getTimestamp() != null ? getTimestamp().equals(that.getTimestamp()) : that.getTimestamp() == null;
    }

    @Override
    public int hashCode() {
        int result = getId() != null ? getId().hashCode() : 0;
        result = 31 * result + (getTopic() != null ? getTopic().hashCode() : 0);
        result = 31 * result + (getKeySchemaName() != null ? getKeySchemaName().hashCode() : 0);
        result = 31 * result + (getValueSchemaName() != null ? getValueSchemaName().hashCode() : 0);
        result = 31 * result + (getTimestamp() != null ? getTimestamp().hashCode() : 0);
        return result;
    }

}
