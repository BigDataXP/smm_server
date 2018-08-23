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
package com.hortonworks.smm.kafka.webservice.resources.metrics;

import com.codahale.metrics.annotation.Timed;
import com.google.inject.Inject;
import com.hortonworks.smm.kafka.services.metric.AggregatedMetricsService;
import com.hortonworks.smm.kafka.services.metric.TimeSpan;
import com.hortonworks.smm.kafka.services.metric.dtos.AggrConsumerMetrics;
import com.hortonworks.smm.kafka.services.metric.dtos.AggrProducerMetrics;
import com.hortonworks.smm.kafka.services.metric.dtos.AggrTopicMetrics;
import com.hortonworks.smm.kafka.services.metric.dtos.BrokerDetails;
import com.hortonworks.smm.kafka.services.metric.dtos.ClusterWithBrokerMetrics;
import com.hortonworks.smm.kafka.services.metric.dtos.ClusterWithTopicMetrics;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;
import java.util.Collection;
import java.util.Objects;

import static com.hortonworks.smm.kafka.services.metric.TimeSpan.ALLOWED_VALUES;

@Path("/api/v1/admin/metrics/aggregated")
@Api(description = "End point for aggregated Broker, Topic, Producer and Consumer metrics",
        tags = "Aggregated metric operations")
@Produces(MediaType.APPLICATION_JSON)
public class AggregatedMetricsResource {

    private final AggregatedMetricsService aggregatedMetricsService;

    @Inject
    public AggregatedMetricsResource(AggregatedMetricsService aggregatedMetricsService) {

        Objects.requireNonNull(aggregatedMetricsService, "aggregatedMetricsService shouldn't be null");

        this.aggregatedMetricsService = aggregatedMetricsService;
    }

    @GET
    @Path("/topics")
    @ApiOperation(value = "Get cluster with topic metrics",
            response = ClusterWithTopicMetrics.class)
    @Timed
    public ClusterWithTopicMetrics getClusterWithTopicMetrics(@ApiParam(value = "Pre-defined Time duration",
                                                                        allowableValues = ALLOWED_VALUES)
                                                              @QueryParam("duration") String duration,
                                                              @ApiParam(value = "State of the Client",
                                                                        allowableValues = "active, inactive, all")
                                                              @QueryParam("state") String state,
                                                              @Context SecurityContext securityContext) {
        TimeSpan timeSpan = new TimeSpan(TimeSpan.TimePeriod.valueOf(duration));
        return aggregatedMetricsService.getAggrTopicsMetricsAtClusterScope(state, timeSpan, securityContext);
    }

    @GET
    @Path("/topics/{topicName}")
    @ApiOperation(value = "Get topic metrics for the given topic",
            response = AggrTopicMetrics.class)
    @Timed
    public AggrTopicMetrics getTopicMetrics(@ApiParam("Name of the topic") @PathParam("topicName") String topicName,
                                            @ApiParam(value = "Pre-defined Time duration", allowableValues = ALLOWED_VALUES)
                                            @QueryParam("duration") String duration,
                                            @ApiParam(value = "State of the Client",
                                                    allowableValues = "active, inactive, all")
                                            @QueryParam("state") String state,
                                            @Context SecurityContext securityContext) {
        TimeSpan timeSpan = new TimeSpan(TimeSpan.TimePeriod.valueOf(duration));
        return aggregatedMetricsService.getTopicMetrics(topicName, state, timeSpan, securityContext);
    }

    @GET
    @Path("/brokers")
    @ApiOperation(value = "Get cluster with broker metrics",
            response = ClusterWithBrokerMetrics.class)
    @Timed
    public ClusterWithBrokerMetrics getClusterWithBrokerMetrics(@ApiParam(value = "Pre-defined Time duration",
                                                                                   allowableValues = ALLOWED_VALUES)
                                                                         @QueryParam("duration") String duration,
                                                                         @Context SecurityContext securityContext) {

        TimeSpan timeSpan = new TimeSpan(TimeSpan.TimePeriod.valueOf(duration));
        return aggregatedMetricsService.getAggrBrokerMetricsAtClusterScope(timeSpan);
    }

    @GET
    @Path("/brokers/{brokerId}")
    @ApiOperation(value = "Get broker details with metrics for a given brokerId",
            response = BrokerDetails.class)
    @Timed
    public BrokerDetails getBrokerDetails(@ApiParam("Broker id") @PathParam("brokerId") Integer brokerId,
                                          @ApiParam(value = "Pre-defined Time duration", allowableValues = ALLOWED_VALUES)
                                                   @QueryParam("duration") String duration,
                                          @Context SecurityContext securityContext) {

        TimeSpan timeSpan = new TimeSpan(TimeSpan.TimePeriod.valueOf(duration));
        return aggregatedMetricsService.brokerDetails(brokerId, timeSpan, securityContext);
    }

    @GET
    @Path("/producers")
    @ApiOperation(value = "Get producer metrics for all the alive producers",
            response = AggrProducerMetrics.class,
            responseContainer = "List")
    @Timed
    public Collection<AggrProducerMetrics> getAllProducerMetrics(@ApiParam(value = "Pre-defined Time duration", allowableValues = ALLOWED_VALUES)
                                                                 @QueryParam("duration") String duration,
                                                                 @ApiParam(value = "State of the Client",
                                                                         allowableValues = "active, inactive, all")
                                                                 @QueryParam("state") String state,
                                                                 @Context SecurityContext securityContext) {
        TimeSpan timeSpan = new TimeSpan(TimeSpan.TimePeriod.valueOf(duration));
        return aggregatedMetricsService.getAllProducerMetrics(state, timeSpan, securityContext);
    }

    @GET
    @Path("/producers/{producerClientId}")
    @ApiOperation(value = "Get producer metrics for the given producerClientId",
            response = AggrProducerMetrics.class)
    @Timed
    public AggrProducerMetrics getProducerMetrics(@ApiParam("Producer client identifier")
                                                  @PathParam("producerClientId") String producerClientId,
                                                  @ApiParam(value = "Pre-defined Time duration", allowableValues = ALLOWED_VALUES)
                                                  @QueryParam("duration") String duration,
                                                  @Context SecurityContext securityContext) {
        TimeSpan timeSpan = new TimeSpan(TimeSpan.TimePeriod.valueOf(duration));
        return aggregatedMetricsService.getProducerMetrics(producerClientId, timeSpan, securityContext);
    }



    @GET
    @Path("/groups")
    @ApiOperation(value = "Get all consumer group details.",
            response = AggrConsumerMetrics.class,
            responseContainer = "List")
    @Timed
    public Collection<AggrConsumerMetrics> getAllConsumerGroupMetrics(@ApiParam(value = "Pre-defined Time duration", allowableValues = ALLOWED_VALUES)
                                                                 @QueryParam("duration") String duration,
                                                                 @ApiParam("Whether to fetch timeline metrics")
                                                                 @QueryParam("requireTimelineMetrics")
                                                                         boolean requireTimelineMetrics,
                                                                 @ApiParam(value = "State of the Client",
                                                                         allowableValues = "active, inactive, all")
                                                                     @QueryParam("state") String state,
                                                                @Context SecurityContext securityContext) {
        TimeSpan timeSpan = new TimeSpan(TimeSpan.TimePeriod.valueOf(duration));
        return aggregatedMetricsService.getAllConsumerMetrics(requireTimelineMetrics, state, timeSpan, securityContext);
    }

    @GET
    @Path("/groups/{groupName}")
    @ApiOperation(value = "Get consumer group details for the given groupName.",
            response = AggrConsumerMetrics.class)
    @Timed
    public AggrConsumerMetrics getConsumerGroupMetrics(@ApiParam("Name of the consumer group")
                                                  @PathParam("groupName") String groupName,
                                                  @ApiParam(value = "Pre-defined Time duration", allowableValues = ALLOWED_VALUES)
                                                  @QueryParam("duration") String duration,
                                                  @ApiParam("Whether to fetch timeline metrics")
                                                  @QueryParam("requireTimelineMetrics")
                                                          boolean requireTimelineMetrics,
                                                  @ApiParam(value = "State of the Producer",
                                                          allowableValues = "active, inactive, all")
                                                      @QueryParam("state") String state,
                                                    @Context SecurityContext securityContext) {
        TimeSpan timeSpan = new TimeSpan(TimeSpan.TimePeriod.valueOf(duration));
        return aggregatedMetricsService.getConsumerMetrics(groupName, requireTimelineMetrics, state, timeSpan, securityContext);
    }

}
