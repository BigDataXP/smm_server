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

package com.hortonworks.smm.kafka.webservice.resources.serdes;

import com.codahale.metrics.annotation.Timed;
import com.google.inject.Inject;
import com.hortonworks.smm.kafka.services.message.TopicMessageService;
import com.hortonworks.smm.kafka.services.message.dtos.SerdesMappings;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.Objects;

@Path("/api/v1/admin/serdes")
@Api(value = "/v1", description = "Endpoint for managing serdes")
@Produces(MediaType.APPLICATION_JSON)
public class SerdesResource {

    private static final String DESCRIPTION = "Serdes related operations";

    private TopicMessageService topicMessageService;

    @Inject
    public SerdesResource(TopicMessageService topicMessageService) {
        Objects.requireNonNull(topicMessageService, "topicMessageService must not be null");

        this.topicMessageService = topicMessageService;
    }

    @GET
    @Path("/")
    @ApiOperation(value = "List of supported serdes",
                  response = SerdesMappings.class,
                  tags = DESCRIPTION)
    @Timed
    public SerdesMappings getSupportedSerdes() {
        return topicMessageService.getSerdesMapping();
    }
}
