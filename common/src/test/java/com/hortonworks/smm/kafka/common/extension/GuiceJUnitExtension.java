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

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestReporter;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.util.ArrayList;
import java.util.Optional;

/**
 * Provides method level injection for a given test environment (UNIT,INTEGRATION etc) for any method annotated with
 *
 * @TestTemplate and invoked with extension @KafkaAdminTestExtension
 */
public class GuiceJUnitExtension implements ParameterResolver {

    private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace.GLOBAL;
    private Injector injector;

    public GuiceJUnitExtension(Injector injector) {
        this.injector = injector;
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {

        // Argument passed to the test function is which is not of type TestReporter nor TestInfo
        // will resolved by calling resolveParameter()

        Class parameterType = parameterContext.getParameter().getType();
        if (parameterType == TestReporter.class || parameterType == TestInfo.class) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {

        Object object;

        if (parameterContext.getParameter().isAnnotationPresent(Named.class)) {
            Named namedAnnotation = parameterContext.getParameter().getAnnotation(Named.class);
            Key key = Key.get(parameterContext.getParameter().getParameterizedType(), Names.named(namedAnnotation.value()));
            object = injector.getInstance(key);
        } else {
            object = injector.getInstance(parameterContext.getParameter().getType());
        }

        // If an object is AutoCloseable then store the object at current ClassLevel store so that KafkaAdminTestExtension
        // can close the object after all the test cases for that class
        if (object instanceof AutoCloseable) {
            Optional<ExtensionContext> testTemplateExtension = extensionContext.getParent();
            if (!testTemplateExtension.isPresent())
                throw new RuntimeException("Failed to obtain TestTemplateExtensionContext");
            Optional<ExtensionContext> classExtensionContext = testTemplateExtension.get().getParent();
            if (!classExtensionContext.isPresent())
                throw new RuntimeException("Failed to obtain ClassExtensionContext");
            ArrayList<AutoCloseable> closeableObjects = (ArrayList<AutoCloseable>) classExtensionContext.get().getStore(NAMESPACE).
                    getOrComputeIfAbsent(AbstractKafkaAdminTestExtension.CLOSEABLE_OBJECTS_STORE_KEY, key -> new ArrayList<>());
            closeableObjects.add((AutoCloseable) object);
            extensionContext.getStore(NAMESPACE).put(AbstractKafkaAdminTestExtension.CLOSEABLE_OBJECTS_STORE_KEY, closeableObjects);
        }

        return object;
    }

}
