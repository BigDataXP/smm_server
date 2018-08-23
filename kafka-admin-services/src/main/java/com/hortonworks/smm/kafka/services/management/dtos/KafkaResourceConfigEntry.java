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
package com.hortonworks.smm.kafka.services.management.dtos;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.clients.admin.ConfigEntry;

import java.util.Objects;

/**
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class KafkaResourceConfigEntry {

    @JsonProperty
    private String name;

    @JsonProperty
    private String value;

    private Boolean isDefault;

    private Boolean isSensitive;

    private Boolean isReadOnly;

    private KafkaResourceConfigEntry() {
    }

    public KafkaResourceConfigEntry(String name, String value, boolean isDefault, boolean isSensitive, boolean isReadOnly) {
        this.name = name;
        this.value = value;
        this.isDefault = isDefault;
        this.isSensitive = isSensitive;
        this.isReadOnly = isReadOnly;
    }

    public static KafkaResourceConfigEntry from(ConfigEntry configEntry) {
        return new KafkaResourceConfigEntry(configEntry.name(),
                                            configEntry.value(),
                                            configEntry.isDefault(),
                                            configEntry.isSensitive(),
                                            configEntry.isReadOnly());
    }

    public String name() {
        return name;
    }

    public String value() {
        return value;
    }

    @JsonProperty("isDefault")
    public Boolean isDefault() {
        return isDefault;
    }

    @JsonProperty("isSensitive")
    public Boolean isSensitive() {
        return isSensitive;
    }

    @JsonProperty("isReadOnly")
    public Boolean isReadOnly() {
        return isReadOnly;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KafkaResourceConfigEntry that = (KafkaResourceConfigEntry) o;
        return isDefault == that.isDefault &&
                isSensitive == that.isSensitive &&
                isReadOnly == that.isReadOnly &&
                Objects.equals(name, that.name) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value, isDefault, isSensitive, isReadOnly);
    }

    @Override
    public String toString() {
        return "Entry{" +
                "name='" + name + '\'' +
                ", value='" + value + '\'' +
                ", isDefault=" + isDefault +
                ", isSensitive=" + isSensitive +
                ", isReadOnly=" + isReadOnly +
                '}';
    }
}
