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
package com.hortonworks.smm.kafka.services.management;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kafka.clients.admin.AdminClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public abstract class AbstractManagementServiceCache implements AutoCloseable {
    public static final Logger LOG = LoggerFactory.getLogger(AbstractManagementServiceCache.class);

    private static final long DEFAULT_SCHEDULER_SHUTDOWN_TIMEOUT_MS = 5 * 60 * 1000L;
    private static final int DEFAULT_ADMIN_CLIENT_TIMEOUT_MS = 120 * 1000;
    private int adminClientTimeoutMs;

    protected AdminClient adminClient;
    protected ScheduledExecutorService scheduler;

    public AbstractManagementServiceCache(AdminClient adminClient,
                                          String schedulerName,
                                          Integer adminClientTimeOutMs,
                                          Long cacheRefreshIntervalMs) {
        Objects.requireNonNull(adminClient, "adminClient must not be null");

        this.adminClient = adminClient;
        LOG.info("Admin client created successfully for [{}]", this);

        adminClientTimeoutMs = adminClientTimeOutMs == null ? DEFAULT_ADMIN_CLIENT_TIMEOUT_MS : adminClientTimeOutMs;
        LOG.debug("Time out to fetch future results in management service is {} ms", adminClientTimeoutMs);

        scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                                                                   .setNameFormat(schedulerName + "-%d")
                                                                   .setDaemon(true)
                                                                   .build());

        scheduler.scheduleWithFixedDelay(() -> {
                    Long startTime = System.currentTimeMillis();
                    try {
                        syncCache();
                        LOG.debug("Refreshed cache for {} in {} ms", schedulerName, (System.currentTimeMillis() - startTime));
                    } catch (Exception e) {
                        LOG.error("Failed to refresh cache for " + schedulerName, e);
                    }
                }, cacheRefreshIntervalMs, cacheRefreshIntervalMs, TimeUnit.MILLISECONDS);
    }

    protected <T> T resultFromFuture(Future<T> future) {
        try {
            return future.get(adminClientTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    abstract public void syncCache();

    @Override
    public void close() throws Exception {
        if (adminClient != null) {
            adminClient.close();
            LOG.info("Admin client closed successfully for [{}]", this);
        }

        scheduler.shutdown();
        scheduler.awaitTermination(DEFAULT_SCHEDULER_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }
}
