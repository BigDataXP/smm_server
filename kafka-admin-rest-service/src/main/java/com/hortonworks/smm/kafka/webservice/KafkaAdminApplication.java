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
package com.hortonworks.smm.kafka.webservice;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.hortonworks.registries.common.ServletFilterConfiguration;
import com.hortonworks.registries.storage.TransactionManager;
import com.hortonworks.registries.storage.transaction.TransactionEventListener;
import com.hortonworks.smm.kafka.services.Service;
import com.hortonworks.smm.kafka.services.security.AuthorizerConfiguration;
import com.hortonworks.smm.kafka.services.security.SMMAuthorizer;
import com.hortonworks.smm.kafka.services.security.auth.SMMSecurityContextRequestFilter;
import com.hortonworks.smm.kafka.services.security.impl.NoopAuthorizer;
import com.hortonworks.smm.kafka.webservice.common.SMMGenericExceptionMapper;
import com.hortonworks.smm.kafka.webservice.config.SMMConfig;
import com.hortonworks.smm.kafka.webservice.dtos.SMMVersion;
import com.hortonworks.smm.kafka.webservice.module.KafkaAdminModule;
import com.hortonworks.smm.kafka.webservice.resources.version.VersionResource;
import com.hortonworks.smm.kafka.webservice.resources.management.BrokerManagementResource;
import com.hortonworks.smm.kafka.webservice.resources.management.ClusterManagementResource;
import com.hortonworks.smm.kafka.webservice.resources.management.ConsumerManagementResource;
import com.hortonworks.smm.kafka.webservice.resources.management.KafkaResourceConfigsResource;
import com.hortonworks.smm.kafka.webservice.resources.management.SearchManagementResource;
import com.hortonworks.smm.kafka.webservice.resources.management.TopicManagementResource;
import com.hortonworks.smm.kafka.webservice.resources.message.TopicMessageResource;
import com.hortonworks.smm.kafka.webservice.resources.metrics.AggregatedMetricsResource;
import com.hortonworks.smm.kafka.webservice.resources.metrics.BrokerMetricsResource;
import com.hortonworks.smm.kafka.webservice.resources.metrics.ConsumerMetricsResource;
import com.hortonworks.smm.kafka.webservice.resources.metrics.ProducerMetricsResource;
import com.hortonworks.smm.kafka.webservice.resources.metrics.TopicMetricsResource;
import com.hortonworks.smm.kafka.webservice.resources.schema.SchemaRegistryResource;
import com.hortonworks.smm.kafka.webservice.resources.serdes.SerdesResource;
import io.dropwizard.Application;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.federecio.dropwizard.swagger.SwaggerBundle;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;

import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.FilterRegistration;
import javax.ws.rs.container.ContainerRequestFilter;

import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 *
 */
public class KafkaAdminApplication extends Application<SMMConfig> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaAdminApplication.class);
    private Injector injector;

    @Override
    public void run(SMMConfig smmConfig, Environment environment) throws Exception {
        environment.jersey().register(MultiPartFeature.class);
        environment.jersey().register(SMMGenericExceptionMapper.class);

        registerResources(smmConfig, environment);
        addServletFilters(smmConfig, environment);
    }

    @Override
    public void initialize(Bootstrap<SMMConfig> bootstrap) {
        bootstrap.addBundle(new SwaggerBundle<SMMConfig>() {
            @Override
            protected SwaggerBundleConfiguration getSwaggerBundleConfiguration(SMMConfig SMMConfig) {
                return SMMConfig.getSwaggerBundleConfig();
            }
        });
        super.initialize(bootstrap);
    }

    private void registerResources(SMMConfig smmConfig, Environment environment) throws ClassNotFoundException, IllegalAccessException, InstantiationException {

        // authorizer
        SMMAuthorizer authorizer;
        AuthorizerConfiguration authorizerConf = smmConfig.getAuthorizerConfiguration();
        if (authorizerConf != null) {
            authorizer = ((Class<SMMAuthorizer>) Class.forName(authorizerConf.getClassName())).newInstance();
            authorizer.init(authorizerConf);
            String filterClazzName = authorizerConf.getContainerRequestFilter();
            ContainerRequestFilter filter;
            if (StringUtils.isEmpty(filterClazzName)) {
                filter = new SMMSecurityContextRequestFilter(); // default
            } else {
                filter = ((Class<ContainerRequestFilter>) Class.forName(filterClazzName)).newInstance();
            }
            LOG.info("Registering ContainerRequestFilter: {}", filter.getClass().getCanonicalName());
            environment.jersey().register(filter);
        } else {
            LOG.info("Authorizer config not set, setting noop authorizer");
            authorizer = NoopAuthorizer.class.newInstance();
        }

        Collection<Class> resourceClasses = Arrays.asList(
                ClusterManagementResource.class,
                BrokerManagementResource.class,
                BrokerMetricsResource.class,
                TopicMetricsResource.class,
                ProducerMetricsResource.class,
                ConsumerManagementResource.class,
                ConsumerMetricsResource.class,
                KafkaResourceConfigsResource.class,
                SchemaRegistryResource.class,
                SearchManagementResource.class,
                TopicManagementResource.class,
                TopicMessageResource.class,
                AggregatedMetricsResource.class,
                SerdesResource.class);

        injector = Guice.createInjector(new KafkaAdminModule(smmConfig, authorizer));

        for (Class resourceClass : resourceClasses) {
            environment.jersey().register(injector.getInstance(resourceClass));
            LOG.debug("Registered resource [{}]", resourceClass);
        }

        environment.jersey().register(new TransactionEventListener(injector.getInstance(TransactionManager.class)));

        SMMVersion smmVersion = fetchSMMVersion();
        LOG.info("Streams Messaging Manager starting with version :- {}", smmVersion);
        environment.jersey().register(new VersionResource(smmVersion));

        environment.lifecycle().manage(new Managed() {
            @Override
            public void start() {
            }

            @Override
            public void stop() {
                Long startTime = System.currentTimeMillis();
                for (Class childServiceClass : new Reflections().getSubTypesOf(Service.class)) {
                    // All the service has been declared as singleton, so use the injector to get the instances currently
                    // being used and close it

                    if (!childServiceClass.isInterface() && !Modifier.isAbstract(childServiceClass.getModifiers())) {
                        Service service = (Service) injector.getInstance(childServiceClass);
                        try {
                            service.close();
                            LOG.info("Closed service : "+childServiceClass.getName());
                        } catch (Exception e) {
                            LOG.error("Error occurred while closing service [{}]", service, e);
                        }
                    }
                }
                LOG.info("Took " + (System.currentTimeMillis() - startTime) + " ms to close all the services");
            }
        });
    }

    private SMMVersion fetchSMMVersion() {
        try (InputStream fileInput = KafkaAdminApplication.class.getResourceAsStream("/smm/VERSION")) {
            Properties props = new Properties();
            props.load(fileInput);
            String version = props.getProperty("version", "unknown");
            String revision = props.getProperty("revision", "unknown");
            String timestampAsString = props.getProperty("timestamp");
            Long timestamp = timestampAsString != null ? Long.valueOf(timestampAsString) : null;

            return new SMMVersion(version, revision, timestamp);
        } catch (Exception ie) {
            LOG.warn("Failed to read streams-messaging-manager version file");
        }

        return SMMVersion.UNKNOWN;
    }

    private void addServletFilters(SMMConfig SMMConfig, Environment environment) {
        List<ServletFilterConfiguration> servletFilterConfigurations = SMMConfig.getServletFilters();
        if (servletFilterConfigurations != null && !servletFilterConfigurations.isEmpty()) {
            for (ServletFilterConfiguration servletFilterConfig : servletFilterConfigurations) {
                try {
                    String className = servletFilterConfig.getClassName();
                    Map<String, String> params = servletFilterConfig.getParams();
                    LOG.info("Registering servlet filter [{}]", servletFilterConfig);
                    Class<? extends Filter> filterClass = (Class<? extends Filter>) Class.forName(className);
                    FilterRegistration.Dynamic dynamic = environment.servlets().addFilter(className, filterClass);
                    if (params != null) {
                        dynamic.setInitParameters(params);
                    }
                    dynamic.addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, "/*");
                } catch (Exception e) {
                    LOG.error("Error registering servlet filter {}", servletFilterConfig);
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public Injector getInjector() {
        return injector;
    }

    public static void main(String[] args) throws Exception {
        KafkaAdminApplication kafkaAdminApplication = new KafkaAdminApplication();
        kafkaAdminApplication.run(args);
    }
}
