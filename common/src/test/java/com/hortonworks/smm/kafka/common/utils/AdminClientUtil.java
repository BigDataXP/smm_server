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

package com.hortonworks.smm.kafka.common.utils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

public class AdminClientUtil {

    public static void createTopic(AdminClient adminClient, NewTopic newTopic, long timeOutMs) {
        createTopics(adminClient, Collections.singleton(newTopic), timeOutMs);
    }

    public static void createTopics(AdminClient adminClient, Collection<NewTopic> topicsToCreate, long timeOutMs) {
        try {
            long startTime = System.currentTimeMillis();
            adminClient.createTopics(topicsToCreate);
            boolean createdAllTopics = false;
            while (timeOutMs > 0 && !createdAllTopics) {
                createdAllTopics = true;
                Set<String> allTopics = adminClient.listTopics()
                                                   .listings()
                                                   .get()
                                                   .stream()
                                                   .map(topicListing -> topicListing.name())
                                                   .collect(Collectors.toSet());
                for (NewTopic topic : topicsToCreate) {
                    if (!allTopics.contains(topic.name())) {
                        createdAllTopics = false;
                        break;
                    }
                }

                Thread.sleep(100);

                timeOutMs -= (System.currentTimeMillis() - startTime);
                startTime = System.currentTimeMillis();
            }
            if (!createdAllTopics)
                throw new RuntimeException("Failed to create topics : " + topicsToCreate + "with in the given timeout period");
        } catch (Exception e) {
            throw new RuntimeException("Failed to create topics : " + topicsToCreate, e);
        }
    }

    public static void deleteTopic(AdminClient adminClient, String topicName, long timeOutMs) {
        deleteTopics(adminClient, Collections.singleton(topicName), timeOutMs);
    }

    public static void deleteTopics(AdminClient adminClient, Collection<String> topicsToDelete, long timeOutMs) {
        try {
            long startTime = System.currentTimeMillis();
            adminClient.deleteTopics(topicsToDelete);
            boolean deletedAllTopics = false;
            while (timeOutMs > 0 && !deletedAllTopics) {
                deletedAllTopics = true;
                Set<String> allTopics = adminClient.listTopics()
                                                   .listings()
                                                   .get()
                                                   .stream()
                                                   .map(topicListing -> topicListing.name())
                                                   .collect(Collectors.toSet());
                for (String topic : topicsToDelete) {
                    if (allTopics.contains(topic)) {
                        deletedAllTopics = false;
                        break;
                    }
                }

                Thread.sleep(100);

                timeOutMs -= (System.currentTimeMillis() - startTime);
                startTime = System.currentTimeMillis();
            }
            if (!deletedAllTopics)
                throw new RuntimeException("Failed to create topics : " + topicsToDelete + " with in the given timeout period");
        } catch (Exception e) {
            throw new RuntimeException("Failed to create topics : " + topicsToDelete, e);
        }
    }
}
