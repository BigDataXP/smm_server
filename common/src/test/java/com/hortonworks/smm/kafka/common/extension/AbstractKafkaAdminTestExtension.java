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

package com.hortonworks.smm.kafka.common.extension;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.hortonworks.smm.kafka.common.module.StartableModule;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * Determines the test environment under which a test should be run and maintains the life cycle of the objects created
 * with guice
 */
public abstract class AbstractKafkaAdminTestExtension implements TestTemplateInvocationContextProvider, BeforeAllCallback, AfterAllCallback {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractKafkaAdminTestExtension.class);

    public static final String CLOSEABLE_OBJECTS_STORE_KEY = "CLOSEABLE_OBJECTS";

    private EnumMap<KafkaAdminTestEnvironment, AbstractModule> moduleRegistry;
    private List<Injector> injectorsToRunWith;
    private List<AbstractModule> modulesToRunWith = new ArrayList<>();

    @Override
    public boolean supportsTestTemplate(ExtensionContext context) {
        return true;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
        return injectorsToRunWith.stream().map(injector -> invocationContext(injector));
    }

    private TestTemplateInvocationContext invocationContext(Injector injector) {

        return new TestTemplateInvocationContext() {
            @Override
            public String getDisplayName(int invocationIndex) {
                return String.format("'%s' test mode", injector.getInstance(KafkaAdminTestEnvironment.class));
            }

            @Override
            public List<Extension> getAdditionalExtensions() {
                return Arrays.asList(new GuiceJUnitExtension(injector));
            }
        };
    }

    protected abstract EnumMap<KafkaAdminTestEnvironment, AbstractModule> provideModules();

    @Override
    public void beforeAll(ExtensionContext context) {

        //Register modules
        this.moduleRegistry = provideModules();
        String testEnvString = System.getProperty("testEnv");
        if (testEnvString == null || testEnvString.equals("")) {
            modulesToRunWith.addAll(moduleRegistry.values());
        } else {
            for (String testEnv : testEnvString.split(",")) {
                KafkaAdminTestEnvironment env;
                try {
                    env = KafkaAdminTestEnvironment.valueOf(testEnv);
                } catch (IllegalArgumentException e) {
                    throw new RuntimeException(testEnv + " is not registered as a valid test environemt");
                }

                if (!moduleRegistry.containsKey(env)) {
                    throw new RuntimeException("No guice module is registered with testEnv : " + testEnv);
                }

                modulesToRunWith.add(moduleRegistry.get(env));
            }
        }

        // Call start() of the modules registered
        Map<String, Object> props = buildProps(context.getTestClass().get());
        for (AbstractModule abstractModule : modulesToRunWith) {
            if (abstractModule instanceof StartableModule) {
                LOG.info("Starting up services for module : {}", abstractModule);
                ((StartableModule) abstractModule).start(props);
            }
        }

        // Create guice injectors from the registered modules
        injectorsToRunWith = modulesToRunWith.stream().map(module -> Guice.createInjector(module)).collect(Collectors.toList());
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {

        LOG.info("Closing all the objects captured by GuiceJUnitExtension");

        ArrayList<AutoCloseable> closeableSet = (ArrayList<AutoCloseable>) context.getStore(ExtensionContext.Namespace.GLOBAL).get(CLOSEABLE_OBJECTS_STORE_KEY);
        if (closeableSet != null) {
            for (AutoCloseable autoCloseable : closeableSet) {
                autoCloseable.close();
            }
        }

        Map<String, Object> props = buildProps(context.getTestClass().get());
        for (AbstractModule abstractModule : modulesToRunWith) {
            if (abstractModule instanceof StartableModule) {
                ((StartableModule) abstractModule).stop(props);
                LOG.info("Stopped services for module : {}", abstractModule);
            }
        }
    }

    protected abstract Map<String, Object> buildProps(Class<?> klass);

}
