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
package com.hortonworks.smm.kafka.services.authorization;

import com.hortonworks.smm.kafka.common.utils.AdminClientUtil;
import com.hortonworks.smm.kafka.services.extension.KafkaAdminServiceTest;
import com.hortonworks.smm.kafka.services.management.TopicManagementService;
import com.hortonworks.smm.kafka.services.management.dtos.TopicInfo;
import com.hortonworks.smm.kafka.services.security.AuthorizerConfiguration;
import com.hortonworks.smm.kafka.services.security.KafkaAuthorizerConfiguration;
import com.hortonworks.smm.kafka.services.security.SMMAuthorizer;
import com.hortonworks.smm.kafka.services.security.SMMPrincipal;
import com.hortonworks.smm.kafka.services.security.SecurityUtil;
import com.hortonworks.smm.kafka.services.security.auth.SMMSecurityContext;
import com.hortonworks.smm.kafka.services.security.impl.DefaultSMMAuthorizer;
import javax.ws.rs.core.SecurityContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestTemplate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This test starts a Kafka cluster and creates 7 topics. We plug in a
 * Custom Ranger Implementation that enforces some authorization rules/policies:
 * <p>
 * - The "IT" group can do anything
 * - The "testgroup" group can describe topic-1...topic-4 topics
 * - The "devgroup" group can describe topic-5...topic-7 topics
 * <p>
 * These rules/polices are stored in a json file.
 * <p>
 * We create three users (admin, testuser, devuser) for each of this group and run
 * authorization filter on all topic info..
 */

@KafkaAdminServiceTest(numBrokerNodes = 3)
@DisplayName("Basic test to validate authorization API.")
public class BasicRangerIntegrationTest {

    private static final int TIME_OUT = 60 * 1000;

    @TestTemplate
    public void testTopicFiltering(AdminClient adminClient, TopicManagementService topicManagementService) throws Exception {
        List<String> topicNames = IntStream.range(1, 8).boxed().map(x -> "topic-" + x).collect(Collectors.toList());

        final List<NewTopic> topicsToCreate = new ArrayList<>();
        for (String topic : topicNames) {
            topicsToCreate.add(new NewTopic(topic, 3, (short) 2));
        }

        AdminClientUtil.createTopics(adminClient, topicsToCreate, TIME_OUT);
        topicManagementService.syncCache();

        Collection<TopicInfo> allTopics = topicManagementService.topicInfos(topicNames);

        // Create users for testing
        UserGroupInformation.createUserForTesting("admin", new String[] {"IT"});
        UserGroupInformation.createUserForTesting("devuser", new String[] {"devgroup"});
        UserGroupInformation.createUserForTesting("testuser", new String[] {"testgroup"});

        //Create RangerKafkaAuthorizer with custom implementation
        SMMAuthorizer authorizer = getDefaultSMMAuthorizer();

        //Filter using admin user, this should return all topics
        SMMPrincipal principal = new SMMPrincipal("admin");
        SecurityContext securityContext = new SMMSecurityContext(principal, "http", "BASIC");
        Collection<TopicInfo> filteredTopics = SecurityUtil.filterTopics(authorizer,
            securityContext,
            allTopics,
            TopicInfo::name);

        Assertions.assertEquals(allTopics, filteredTopics);

        //Filter using testuser, this should return four topics
        principal = new SMMPrincipal("testuser");
        securityContext = new SMMSecurityContext(principal, "http", "BASIC");
        filteredTopics = SecurityUtil.filterTopics(authorizer,
            securityContext,
            allTopics,
            TopicInfo::name);

        Set<String> expected = new HashSet<>(Arrays.asList("topic-1", "topic-2", "topic-3", "topic-4"));
        Assertions.assertTrue(filteredTopics.size() == 4, "count mismatch for testuser");
        Assertions.assertEquals(expected, filteredTopics.stream().map(TopicInfo::name).collect(Collectors.toSet()));

        //Filter using devuser, this should return three topics
        principal = new SMMPrincipal("devuser");
        securityContext = new SMMSecurityContext(principal, "http", "BASIC");
        filteredTopics = SecurityUtil.filterTopics(authorizer,
            securityContext,
            allTopics,
            TopicInfo::name);

        expected = new HashSet<>(Arrays.asList("topic-6", "topic-7", "topic-5"));
        Assertions.assertTrue(filteredTopics.size() == 3, "count mismatch for devuser");
        Assertions.assertEquals(expected, filteredTopics.stream().map(TopicInfo::name).collect(Collectors.toSet()));

        AdminClientUtil.deleteTopics(adminClient, topicNames, TIME_OUT);
    }

    private SMMAuthorizer getDefaultSMMAuthorizer() {
        SMMAuthorizer authorizer = new DefaultSMMAuthorizer();

        AuthorizerConfiguration authorizerConfiguration = new AuthorizerConfiguration();
        KafkaAuthorizerConfiguration kafkaAuthorizerConfiguration = new KafkaAuthorizerConfiguration();

        kafkaAuthorizerConfiguration.setClassName("org.apache.ranger.authorization.kafka.authorizer.RangerKafkaAuthorizer");

        Map<String, Object> authorizerConfigs = new HashMap<>();
        kafkaAuthorizerConfiguration.setProperties(authorizerConfigs);

        authorizerConfiguration.setKafkaAuthorizerConfiguration(kafkaAuthorizerConfiguration);
        authorizer.init(authorizerConfiguration);
        return authorizer;
    }

}
