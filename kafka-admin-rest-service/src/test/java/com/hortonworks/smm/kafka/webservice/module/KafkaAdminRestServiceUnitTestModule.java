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

package com.hortonworks.smm.kafka.webservice.module;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.hortonworks.smm.kafka.common.config.KafkaAdminClientConfig;
import com.hortonworks.smm.kafka.common.extension.KafkaAdminTestConstants;
import com.hortonworks.smm.kafka.common.extension.KafkaAdminTestEnvironment;
import com.hortonworks.smm.kafka.common.module.StartableModule;
import com.hortonworks.smm.kafka.services.EmbeddedKafkaClusterWrapper;
import com.hortonworks.smm.kafka.webservice.util.SMMConfigUtil;
import com.hortonworks.smm.kafka.webservice.wrapper.LocalKafkaAdminServer;
import org.apache.kafka.clients.admin.AdminClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaAdminRestServiceUnitTestModule extends AbstractModule implements StartableModule {

    private Logger LOG = LoggerFactory.getLogger(KafkaAdminRestServiceUnitTestModule.class);
    private EmbeddedKafkaClusterWrapper KAFKA_CLUSTER = null;
    private LocalKafkaAdminServer localKafkaAdminServer = null;

    @Override
    protected void configure() {
    }

    @Override
    public void start(Map<String, Object> props) {

        int numBrokerNodes = Integer.parseInt(props.get(KafkaAdminTestConstants.NUM_BROKER_NODES).toString());
        if (numBrokerNodes != 0) {
            String[] kafkaConfig = (String[]) props.get(KafkaAdminTestConstants.KAFKA_CONFIG);
            if (kafkaConfig.length != 0) {
                Properties properties = new Properties();
                for (int i = 0; i < kafkaConfig.length; i++) {
                    String[] keyValueString = kafkaConfig[i].split("=");
                    properties.put(keyValueString[0], keyValueString[1]);
                }
                KAFKA_CLUSTER = new EmbeddedKafkaClusterWrapper(numBrokerNodes, properties);
            } else {
                KAFKA_CLUSTER = new EmbeddedKafkaClusterWrapper(numBrokerNodes);
            }
            KAFKA_CLUSTER.startUp();
        }

        String bootstrapServerURL = KAFKA_CLUSTER == null ? "" : KAFKA_CLUSTER.bootstrapServers();
        String resolvedYAMLPath;
        try {
            resolvedYAMLPath = SMMConfigUtil.getSMMConfig("smm-test.yaml", bootstrapServerURL).getPath();
        } catch (IOException e) {
            throw new RuntimeException("Failed to populate bootstrapServerPath in smm-test.yaml", e);
        }

        localKafkaAdminServer = new LocalKafkaAdminServer(resolvedYAMLPath);
        try {
            localKafkaAdminServer.start();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start up local kafka admin server", e);
        }
    }

    @Override
    public void stop(Map<String, Object> props) {
        try {
            localKafkaAdminServer.stop();
        } catch (Exception e) {
            LOG.error("Failed to shutdown local kafka admin server", e);
        }
        if (KAFKA_CLUSTER != null)
            KAFKA_CLUSTER.tearDown();
    }

    @Provides
    public LocalKafkaAdminServer providesLocalKafkaAdminServer() {
        return localKafkaAdminServer;
    }

    @Provides
    public KafkaAdminTestEnvironment providesKafkaAdminTestEnvironment() {
        return KafkaAdminTestEnvironment.UNIT;
    }

    @Provides
    @Singleton
    public KafkaAdminClientConfig providesKafkaAdminClientConfig() {
        Map<String,Object> properties = new HashMap<>();
        properties.put("properties", Collections.EMPTY_MAP);
        return new KafkaAdminClientConfig(KAFKA_CLUSTER.bootstrapServers(), properties);
    }

    @Provides
    public AdminClient providesAdminClient(KafkaAdminClientConfig kafkaAdminClientConfig) {
        return AdminClient.create(kafkaAdminClientConfig.getConfig());
    }
}