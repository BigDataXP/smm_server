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
import com.hortonworks.smm.kafka.services.management.ResourceConfigsService;
import com.hortonworks.smm.kafka.services.management.dtos.KafkaResourceConfig;
import com.hortonworks.smm.kafka.services.security.SMMAuthorizer;
import com.hortonworks.smm.kafka.services.security.SecurityUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 *
 */
@Path("/api/v1/admin/configs")
@Api(description = "End point for all Kafka admin operations", tags = "Resource configuration operations")
@Produces(MediaType.APPLICATION_JSON)
public class KafkaResourceConfigsResource {

    private final ResourceConfigsService resourceConfigsService;
    private final SMMAuthorizer authorizer;

    @Inject
    public KafkaResourceConfigsResource(ResourceConfigsService resourceConfigsService, SMMAuthorizer authorizer) {
        Objects.requireNonNull(resourceConfigsService, "resourceConfigsService must not be null");
        Objects.requireNonNull(authorizer, "authorizer must not be null");

        this.resourceConfigsService = resourceConfigsService;
        this.authorizer = authorizer;
    }

    @GET
    @Path("/topics")
    @ApiOperation(value = "Get kafka configurations for all topics.",
            response = KafkaResourceConfig.class,
            responseContainer = "List")
    @Timed
    public Collection<KafkaResourceConfig> getAllTopicConfigs(@Context SecurityContext securityContext) {
        return SecurityUtil.filterTopics(authorizer,
            securityContext,
            resourceConfigsService.allTopicConfigs(),
            KafkaResourceConfig::name);
    }

    @GET
    @Path("/topics/{topicName}")
    @ApiOperation(value = "Get kafka configurations for a topic with given topicName.",
            response = KafkaResourceConfig.class)
    @Timed
    public KafkaResourceConfig getTopicConfigs(@PathParam("topicName") String topicName, @Context SecurityContext securityContext) {
        if (SecurityUtil.authorizeTopicDescribe(authorizer, securityContext, topicName)) {
            Collection<KafkaResourceConfig> kafkaResourceConfigs = resourceConfigsService.topicConfigs(Collections.singleton(topicName));
            if(!kafkaResourceConfigs.isEmpty()) {
                return kafkaResourceConfigs.iterator().next();
            }
        }

        throw new javax.ws.rs.NotFoundException("topic ["+ topicName + "] not found");
    }

    @GET
    @Path("/brokers")
    @ApiOperation(value = "Get kafka configurations for the given brokerIds. if brokerIds parameter does not have any " +
            "value then configuration for all brokers is returned.",
            response = KafkaResourceConfig.class,
            responseContainer = "List")
    @Timed
    public Collection<KafkaResourceConfig> getAllBrokerDetails() {
        return resourceConfigsService.allBrokerConfigs();
    }

    @GET
    @Path("/brokers/{brokerId}")
    @ApiOperation(value = "Get kafka configurations for the given brokerId.",
            response = KafkaResourceConfig.class)
    @Timed
    public KafkaResourceConfig getBrokerDetails(@PathParam("brokerId") String brokerId) {
        Collection<KafkaResourceConfig> kafkaResourceConfigs = resourceConfigsService.brokerConfigs(Collections.singleton(Integer.parseInt(brokerId)));
        if(!kafkaResourceConfigs.isEmpty()) {
           return kafkaResourceConfigs.iterator().next();
        }

        throw new javax.ws.rs.NotFoundException(" Broker with id : [" + brokerId + "] not found");
    }
}
