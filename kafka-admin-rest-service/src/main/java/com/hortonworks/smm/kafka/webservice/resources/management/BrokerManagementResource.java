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

import com.google.inject.Inject;
import com.hortonworks.smm.kafka.services.management.BrokerManagementService;
import com.hortonworks.smm.kafka.services.management.dtos.BrokerNode;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Path("/api/v1/admin/brokers")
@Api(description = "End point for Kafka admin operations about brokers", tags="Brokers metadata operations")
@Produces(MediaType.APPLICATION_JSON)
public class BrokerManagementResource {

    private BrokerManagementService brokerManagementService;

    @Inject
    public BrokerManagementResource(BrokerManagementService brokerManagementService) {
        Objects.requireNonNull(brokerManagementService, "brokerManagementService must not be null");

        this.brokerManagementService = brokerManagementService;
    }

    @GET
    @Path("/{brokerId}")
    @ApiOperation(value = "Returns broker node in the cluster with the given broker Id.",
            response = BrokerNode.class)
    public BrokerNode getBroker(@PathParam("brokerId") Integer brokerId) {
        Collection<BrokerNode> brokers = brokerManagementService.brokers(Collections.singleton(brokerId));
        if (brokers.isEmpty()) {
            throw new NotFoundException("Broker with id `" + brokerId + "` not found");
        }

        return brokers.iterator().next();
    }

    @GET
    @Path("/")
    @ApiOperation(value = "Returns all broker nodes in the cluster with the given brokerIds if they exist.",
            response = BrokerNode.class,
            responseContainer = "List")
    public Collection<BrokerNode> getBrokers(
            @ApiParam(value = "Comma separated broker identifiers", example = "1001,1002")
            @QueryParam("brokerIds") String brokerIdValues) {
        Set<Integer> brokerIds = brokerIds(brokerIdValues);
        return brokerIds == null || brokerIds.isEmpty()
               ? brokerManagementService.allBrokers()
               : brokerManagementService.brokers(brokerIds);
    }

    private Set<Integer> brokerIds(String brokerIdValues) {
        return (brokerIdValues == null || brokerIdValues.isEmpty()) ? Collections.emptySet() :
               Arrays.stream(brokerIdValues.split(","))
                     .map(x -> Integer.parseInt(x.trim()))
                     .collect(Collectors.toSet());
    }


}
