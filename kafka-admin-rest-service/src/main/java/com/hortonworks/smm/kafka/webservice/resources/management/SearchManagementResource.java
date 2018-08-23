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

import com.codahale.metrics.annotation.Timed;
import com.google.inject.Inject;
import com.hortonworks.smm.kafka.services.management.BrokerManagementService;
import com.hortonworks.smm.kafka.services.management.ResourceConfigsService;
import com.hortonworks.smm.kafka.services.management.TopicManagementService;
import com.hortonworks.smm.kafka.services.management.dtos.BrokerNode;
import com.hortonworks.smm.kafka.services.management.dtos.KafkaResourceConfig;
import com.hortonworks.smm.kafka.services.management.dtos.TopicInfo;
import com.hortonworks.smm.kafka.services.management.dtos.TopicPartitionInfo;
import com.hortonworks.smm.kafka.services.management.dtos.TopicPartitionInfos;
import com.hortonworks.smm.kafka.services.security.SMMAuthorizer;
import com.hortonworks.smm.kafka.services.security.SecurityUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 */
@Path("/api/v1/admin/search")
@Api(description = "End point for all search operations")
@Produces(MediaType.APPLICATION_JSON)
public class SearchManagementResource {

    private static final String DESCRIPTION = "Search related operations";

    private final ResourceConfigsService resourceConfigsService;
    private final TopicManagementService topicManagementService;
    private final BrokerManagementService brokerManagementService;
    private final SMMAuthorizer authorizer;

    @Inject
    public SearchManagementResource(ResourceConfigsService resourceConfigsService,
                                    TopicManagementService topicManagementService,
                                    BrokerManagementService brokerManagementService,
                                    SMMAuthorizer authorizer) {
        Objects.requireNonNull(resourceConfigsService, "resourceConfigsService must not be null");
        Objects.requireNonNull(topicManagementService, "topicManagementService must not be null");
        Objects.requireNonNull(brokerManagementService, "brokerManagementService must not be null");
        Objects.requireNonNull(authorizer, "authorizer must not be null");

        this.resourceConfigsService = resourceConfigsService;
        this.topicManagementService = topicManagementService;
        this.brokerManagementService = brokerManagementService;
        this.authorizer = authorizer;
    }


    @GET
    @Path("/topics")
    @ApiOperation(value = "Get respective details for given topicNames.",
            response = TopicInfo.class,
            responseContainer = "List",
            tags = DESCRIPTION)
    @Timed
    public Collection<TopicInfo> getTopicDetails(
            @ApiParam(value = "Comma separated topic names", example = "topic1, topic2")
            @QueryParam("topicNames") String topicNames, @Context SecurityContext securityContext) {
        return SecurityUtil.filterTopics(authorizer,
            securityContext,
            topicManagementService.topicInfos(parameterValues(topicNames)),
            TopicInfo::name);
    }

    @GET
    @Path("/topicPartitions")
    @ApiOperation(value = "Get a list of topic partitions with respective details for the given collection of " +
            "topicPartitions which exist in target Kafka cluster.",
            response = TopicPartitionInfos.class,
            tags = DESCRIPTION)
    @Timed
    public TopicPartitionInfos getTopicPartitionInfos(
            @ApiParam(value = "Comma separated topic partition values", example = "events-1, events-2, events-9")
            @QueryParam("partitions") String topicPartitionValues,
            @Context SecurityContext securityContext) {
        Map<String, Collection<TopicPartitionInfo>> partitionInfos =
                topicManagementService.topicPartitionInfos(parameterValues(topicPartitionValues), authorizer, securityContext);
        return new TopicPartitionInfos(partitionInfos);
    }

    @GET
    @Path("/configs/topics")
    @ApiOperation(value = "Get kafka configurations at topic level for given topicNames",
            response = KafkaResourceConfig.class,
            responseContainer = "List",
            tags = DESCRIPTION)
    @Timed
    public Collection<KafkaResourceConfig> getTopicConfigs(
            @ApiParam(value = "Comma separated topic names", example = "topic1, topic2")
            @QueryParam("topicNames") String topicNames,
            @Context SecurityContext securityContext) {
        return SecurityUtil.filterTopics(authorizer,
            securityContext,
            resourceConfigsService.topicConfigs(parameterValues(topicNames)),
            KafkaResourceConfig::name);
    }

    private Collection<String> parameterValues(String values) {
        return (values == null || values.isEmpty()) ? Collections.emptyList() :
                Arrays.stream(values.split(","))
                .map(String::trim)
                .collect(Collectors.toList());
    }

    @GET
    @Path("/configs/brokers")
    @ApiOperation(value = "Get kafka configurations for the given brokerIds.",
            response = KafkaResourceConfig.class,
            responseContainer = "List",
            tags = DESCRIPTION)
    @Timed
    public Collection<KafkaResourceConfig> getBrokerConfigs(
            @ApiParam(value = "Comma separated broker identifiers", example = "1001, 1002")
            @QueryParam("brokerIds") String brokerIdValues) {
        return resourceConfigsService.brokerConfigs(brokerIds(brokerIdValues));
    }

    private Set<Integer> brokerIds(String brokerIdValues) {
        return (brokerIdValues == null || brokerIdValues.isEmpty()) ? Collections.emptySet() :
                Arrays.stream(brokerIdValues.split(","))
                .map(x -> Integer.parseInt(x.trim()))
                .collect(Collectors.toSet());
    }

    @GET
    @Path("/brokers")
    @ApiOperation(value = "Returns broker nodes in the cluster with the given brokerIds if they exist.",
            response = BrokerNode.class,
            responseContainer = "List",
            tags = DESCRIPTION)
    public Collection<BrokerNode> getBrokers(
            @ApiParam(value = "Comma separated broker identifiers", example = "1001, 1002")
            @QueryParam("brokerIds") String brokerIdValues) {
        return brokerManagementService.brokers(brokerIds(brokerIdValues));
    }

}
