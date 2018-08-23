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

package com.hortonworks.smm.kafka.webservice.resources.management;

import com.hortonworks.smm.kafka.services.management.dtos.BrokerNode;
import com.hortonworks.smm.kafka.webservice.extension.KafkaAdminRestServiceTest;
import com.hortonworks.smm.kafka.webservice.wrapper.LocalKafkaAdminServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@KafkaAdminRestServiceTest(numBrokerNodes = 3, kafkaConfig = {})
public class BrokerManagementResourceTest {

    private static String BROKER_MANAGEMENT_RESOURCE_PATH = "/api/v1/admin/brokers/%s";

    @TestTemplate
    public void testGetBroker(LocalKafkaAdminServer localKafkaAdminServer) {
        Response response = localKafkaAdminServer.getWebTarget(buildURL("0")).request().get();
        BrokerNode brokerNode = response.readEntity(BrokerNode.class);

        Assertions.assertTrue(brokerNode.id() == 0 && brokerNode.host().equals("127.0.0.1"));
    }


    @TestTemplate
    public void testGetAllBrokers(LocalKafkaAdminServer localKafkaAdminServer) {
        Response response = localKafkaAdminServer.getWebTarget(buildURL("")).request().get();
        Set<Integer> brokerNodeIds = response.readEntity(new GenericType<Collection<BrokerNode>>() {}).
                                        stream().
                                        map(brokerNode -> brokerNode.id()).collect(Collectors.toSet());

        Assertions.assertEquals(IntStream.range(0, 3).boxed().collect(Collectors.toSet()), brokerNodeIds);
    }

    private String buildURL(String path) {
        return String.format(BROKER_MANAGEMENT_RESOURCE_PATH, path);
    }

}
