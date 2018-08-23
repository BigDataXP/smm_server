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
package com.hortonworks.smm.kafka.services.clients;

import com.hortonworks.smm.kafka.services.clients.dtos.ConsumerGroupInfo;
import com.hortonworks.smm.kafka.services.clients.dtos.ConsumerInfo;
import com.hortonworks.smm.kafka.services.clients.dtos.ConsumerPartitionInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ConsumerGroups {

    static final ConsumerGroups EMPTY = new ConsumerGroups(Collections.emptyMap(), Collections.emptyMap());

    private final Collection<String> consumerGroupNames;

    private final Map<String, ConsumerGroupInfo> consumerGroups;
    private final Collection<ConsumerGroupInfo> activeConsumerGroups;
    private final Collection<ConsumerGroupInfo> inactiveConsumerGroups;

    private final Map<String, List<ConsumerPartitionInfo>> clientToPartitions;

    ConsumerGroups(Map<String, ConsumerGroupInfo> consumerGroups,
                   Map<String, List<ConsumerPartitionInfo>> clientToPartitions) {
        this.consumerGroupNames = Collections.unmodifiableCollection(consumerGroups.keySet());
        this.consumerGroups = Collections.unmodifiableMap(consumerGroups);

        Collection<ConsumerGroupInfo> inactiveConsumerGroups = new ArrayList<>();
        Collection<ConsumerGroupInfo> activeConsumerGroups = new ArrayList<>();
        consumerGroups.values().forEach(cg -> {
            if (cg.active()) {
                activeConsumerGroups.add(cg);
            } else {
                inactiveConsumerGroups.add(cg);
            }
        });
        this.inactiveConsumerGroups = Collections.unmodifiableCollection(inactiveConsumerGroups);
        this.activeConsumerGroups = Collections.unmodifiableCollection(activeConsumerGroups);

        this.clientToPartitions = Collections.unmodifiableMap(clientToPartitions);
    }

    public Collection<String> names() {
        return consumerGroupNames;
    }

    public Collection<ConsumerGroupInfo> all() {
        return consumerGroups.values();
    }

    public Collection<ConsumerGroupInfo> activeConsumerGroups() {
        return activeConsumerGroups;
    }

    public Collection<ConsumerGroupInfo> inactiveConsumerGroups() {
        return inactiveConsumerGroups;
    }

    public ConsumerGroupInfo getGroup(String groupName) {
        return consumerGroups.get(groupName);
    }

    public ConsumerInfo getConsumerInfo(String clientId) {
        return new ConsumerInfo(clientId, clientToPartitions.get(clientId));
    }

    public List<ConsumerInfo> allConsumers() {
        return clientToPartitions
                .entrySet()
                .stream()
                .map(e -> new ConsumerInfo(e.getKey(), e.getValue()))
                .collect(Collectors.toList());

    }
}
