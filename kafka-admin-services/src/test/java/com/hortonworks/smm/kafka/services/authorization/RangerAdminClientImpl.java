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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.ranger.admin.client.RangerAdminClient;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.apache.ranger.plugin.util.ServiceTags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.List;

/**
 * A test implementation of the RangerAdminClient interface that just reads policies in from a file and returns them
 */
public class RangerAdminClientImpl implements RangerAdminClient {
    private static final Logger LOG = LoggerFactory.getLogger(RangerAdminClientImpl.class);
    private final static String cacheFilename = "kafka-policies.json";
    private final static String tagFilename = "kafka-policies-tag.json";
    private Gson gson;

    public void init(String serviceName, String appId, String configPropertyPrefix) {
        Gson gson = null;
        try {
            gson = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").setPrettyPrinting().create();
        } catch(Throwable excp) {
            LOG.error("RangerAdminClientImpl: failed to create GsonBuilder object", excp);
        }
        this.gson = gson;
    }

    public ServicePolicies getServicePoliciesIfUpdated(long lastKnownVersion, long lastActivationTimeInMillis) throws Exception {

        String basedir = System.getProperty("basedir");
        if (basedir == null) {
            basedir = new File(".").getCanonicalPath();
        }

        java.nio.file.Path cachePath = FileSystems.getDefault().getPath(basedir, "/src/test/resources/" + cacheFilename);
        byte[] cacheBytes = Files.readAllBytes(cachePath);

        return gson.fromJson(new String(cacheBytes), ServicePolicies.class);
    }

    public void grantAccess(GrantRevokeRequest request) throws Exception {
        
    }

    public void revokeAccess(GrantRevokeRequest request) throws Exception {
        
    }

    public ServiceTags getServiceTagsIfUpdated(long lastKnownVersion, long lastActivationTimeInMillis) throws Exception {
        return null;
    }

    public List<String> getTagTypes(String tagTypePattern) throws Exception {
        return null;
    }

    
}