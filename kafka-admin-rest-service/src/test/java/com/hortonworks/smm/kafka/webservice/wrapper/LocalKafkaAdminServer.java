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

package com.hortonworks.smm.kafka.webservice.wrapper;

import com.google.inject.Injector;
import com.hortonworks.smm.kafka.webservice.KafkaAdminApplication;
import com.hortonworks.smm.kafka.webservice.config.SMMConfig;
import io.dropwizard.lifecycle.ServerLifecycleListener;
import io.dropwizard.setup.Environment;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

/**
 *  Wrapper over KafkaAdminApplication to control the lifecycle of the streams messaging manager server
 *  with start() and stop() method. See {@link com.hortonworks.smm.kafka.webservice.resources.management.BrokerManagementResourceTest}
 *  for how to use LocalKafkaAdminServer
 */

public class LocalKafkaAdminServer {

    private final LocalKafkaAdminApplication kafkaAdminApplication;

    private WebTarget webTarget;

    /**
     * Creates an instance of {@link LocalKafkaAdminServer} with the given {@code configFilePath}
     *
     * @param configFilePath Path of the config file for this server which contains all the required configuration.
     */
    public LocalKafkaAdminServer(String configFilePath) {
        kafkaAdminApplication = new LocalKafkaAdminApplication(configFilePath);
    }

    /**
     * Starts the server if it is not started yet.
     *
     * @throws Exception
     */
    public void start() throws Exception {
        kafkaAdminApplication.start();
    }

    /**
     * Shuts down the server
     *
     * @throws Exception
     */
    public void stop() throws Exception {
        kafkaAdminApplication.stop();
    }

    public int getLocalPort() {
        return kafkaAdminApplication.getLocalPort();
    }

    public int getAdminPort() {
        return kafkaAdminApplication.getAdminPort();
    }

    public String getLocalURL() {
        return kafkaAdminApplication.localServer.getURI().toString();
    }

    public Injector getInjector() { return kafkaAdminApplication.getInjector();}

    public WebTarget getWebTarget(String url) {
        if (webTarget == null) {
            webTarget = ClientBuilder.newClient().target(this.getLocalURL());
        }
        return webTarget.path(url);
    }

    private static final class LocalKafkaAdminApplication extends KafkaAdminApplication {
        private static final Logger LOG = LoggerFactory.getLogger(LocalKafkaAdminApplication.class);

        private final String configFilePath;
        private volatile Server localServer;

        public LocalKafkaAdminApplication(String configFilePath) {
            this.configFilePath = configFilePath;
        }

        @Override
        public void run(SMMConfig smmConfig, Environment environment) throws Exception {
            super.run(smmConfig, environment);
            environment.lifecycle().addServerLifecycleListener(new ServerLifecycleListener() {
                @Override
                public void serverStarted(Server server) {
                    localServer = server;
                    LOG.info("Received callback as server is started :[{}]", server);
                }
            });
        }

        void start() throws Exception {
            if (localServer == null) {
                LOG.info("Local kafka admin application instance is getting started.");
                run("server", configFilePath);
                LOG.info("Local kafka admin application instance is started at port [{}]", getLocalPort());
            } else {
                LOG.info("Local kafka admin application instance is already started at port [{}]", getLocalPort());
            }
        }

        void stop() throws Exception {
            if (localServer != null) {
                localServer.stop();
                localServer = null;
                LOG.info("Local kafka admin application instance is stopped.");
            } else {
                LOG.info("No local kafka admin application instance is running to be stopped.");
            }
        }

        int getLocalPort() {
            return ((ServerConnector) localServer.getConnectors()[0]).getLocalPort();
        }

        int getAdminPort() {
            return ((ServerConnector) localServer.getConnectors()[1]).getLocalPort();
        }

    }

}
