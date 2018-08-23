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
package com.hortonworks.smm.kafka.webservice.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.hortonworks.registries.common.ServletFilterConfiguration;
import com.hortonworks.registries.storage.StorageProviderConfiguration;
import com.hortonworks.smm.kafka.common.config.KafkaManagementConfig;
import com.hortonworks.smm.kafka.common.config.KafkaMetricsConfig;
import com.hortonworks.smm.kafka.services.security.AuthorizerConfiguration;
import io.dropwizard.Configuration;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;

import java.util.List;
import java.util.Map;

/**
 *
 */

public class SMMConfig extends Configuration {

    @JsonProperty("kafkaBootstrapServers")
    private String kafkaBootstrapServers;

    @JsonProperty("schemaRegistryUrl")
    private String schemaRegistryUrl;

    @JsonProperty("kafkaAdminClient")
    private Map<String, Object> kafkaAdminClientConfig;

    @JsonProperty("kafkaConsumerClient")
    private Map<String, Object> kafkaConsumerClientConfig;

    @JsonProperty("schemaRegistryClient")
    private Map<String, Object> schemaRegistryClientConfig;

    @JsonProperty("kafkaManagementConfig")
    private KafkaManagementConfig kafkaManagementConfig;

    @JsonProperty("kafkaMetricsConfig")
    private KafkaMetricsConfig kafkaMetricsConfig;

    @JsonProperty("storageProviderConfiguration")
    private StorageProviderConfiguration storageProviderConfig;

    @JsonProperty("swagger")
    private SwaggerBundleConfiguration swaggerBundleConfig;

    @JsonProperty("servletFilters")
    private List<ServletFilterConfiguration> servletFilters;

    @JsonProperty("authorizerConfiguration")
    private AuthorizerConfiguration authorizerConfiguration;

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    public Map<String, Object> getKafkaAdminClientConfig() {
        return kafkaAdminClientConfig;
    }

    public Map<String, Object> getKafkaConsumerClientConfig() {
        return kafkaConsumerClientConfig;
    }

    public StorageProviderConfiguration getStorageProviderConfig() {
        return storageProviderConfig;
    }

    public Map<String, Object> getSchemaRegistryClientConfig() {
        return schemaRegistryClientConfig;
    }

    public SwaggerBundleConfiguration getSwaggerBundleConfig() {
        return swaggerBundleConfig;
    }

    public KafkaManagementConfig getKafkaManagementConfig() { return kafkaManagementConfig; }

    public KafkaMetricsConfig getKafkaMetricsConfig() {
        return kafkaMetricsConfig;
    }

    public List<ServletFilterConfiguration> getServletFilters() {
        return servletFilters;
    }

    public AuthorizerConfiguration getAuthorizerConfiguration() {
        return authorizerConfiguration;
    }

    public void setAuthorizerConfiguration(AuthorizerConfiguration authorizerConfiguration) {
        this.authorizerConfiguration = authorizerConfiguration;
    }

    @Override
    public String toString() {
        return "SMMConfig{" +
                ", kafkaBootstrapServers=" + kafkaBootstrapServers +
                ", schemaRegistryUrl=" + schemaRegistryUrl +
                ", kafkaAdminClientConfig=" + kafkaAdminClientConfig +
                ", kafkaConsumerClientConfig=" + kafkaConsumerClientConfig +
                ", schemaRegistryClientConfig=" + schemaRegistryClientConfig +
                ", storageProviderConfig=" + storageProviderConfig +
                ", swaggerBundleConfig=" + swaggerBundleConfig +
                ", servletFilters=" + servletFilters +
                ", kafkaMetricsConfig=" + kafkaMetricsConfig +
                ", authorizerConfiguration=" + authorizerConfiguration +
                ", kafkaManagementConfig=" + kafkaManagementConfig +
                '}';
    }
}