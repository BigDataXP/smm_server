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

package com.hortonworks.smm.kafka.common.config;

import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.smm.kafka.common.errors.SMMConfigurationException;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class KafkaConsumerConfig {

    public static final String POLL_TIMEOUT_MS_PROPERTY_NAME = "poll.timeout.ms";

    private long pollTimeOutMs;

    private Map<String, Object> config;

    public KafkaConsumerConfig(String bootstrapServerString, String schemaRegistryURL, Map<String, Object> config) {
        if (!config.containsKey(POLL_TIMEOUT_MS_PROPERTY_NAME))
            throw new SMMConfigurationException(this.getClass(), POLL_TIMEOUT_MS_PROPERTY_NAME);
        if (bootstrapServerString == null || bootstrapServerString.isEmpty())
            throw new SMMConfigurationException(this.getClass(), "kafkaBootstrapServers");

        this.pollTimeOutMs = Long.parseLong(config.get(POLL_TIMEOUT_MS_PROPERTY_NAME).toString());
        Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerString);
        if (schemaRegistryURL != null && !schemaRegistryURL.isEmpty()) {
            consumerConfig.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), schemaRegistryURL);
        }
        Map<String, Object> propertiesMap = (Map<String, Object>) config.computeIfAbsent("properties", key -> Collections.EMPTY_MAP);
        consumerConfig.putAll(propertiesMap);
        this.config = Collections.unmodifiableMap(consumerConfig);
    }

    public Long getPollTimeOutMs() {
        return pollTimeOutMs;
    }

    public Map<String, Object> getConfig() {
        return config;
    }

    @Override
    public String toString() {
        return "KafkaConsumerConfig{" +
                "pollTimeOutMs=" + pollTimeOutMs +
                ", config=" + config +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KafkaConsumerConfig that = (KafkaConsumerConfig) o;
        return pollTimeOutMs == that.pollTimeOutMs &&
                Objects.equals(config, that.config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pollTimeOutMs, config);
    }
}
