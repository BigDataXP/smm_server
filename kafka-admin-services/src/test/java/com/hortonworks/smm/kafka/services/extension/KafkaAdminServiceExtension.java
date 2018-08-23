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

package com.hortonworks.smm.kafka.services.extension;

import com.google.inject.AbstractModule;
import com.hortonworks.smm.kafka.common.extension.AbstractKafkaAdminTestExtension;
import com.hortonworks.smm.kafka.common.extension.KafkaAdminTestConstants;
import com.hortonworks.smm.kafka.common.extension.KafkaAdminTestEnvironment;
import com.hortonworks.smm.kafka.services.module.KafkaAdminServiceUnitTestModule;


import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

public class KafkaAdminServiceExtension extends AbstractKafkaAdminTestExtension {

    private static EnumMap<KafkaAdminTestEnvironment, AbstractModule> testEnvironmentAbstractModuleEnumMap;

    static {
        testEnvironmentAbstractModuleEnumMap = new EnumMap<>(KafkaAdminTestEnvironment.class);
        testEnvironmentAbstractModuleEnumMap.put(KafkaAdminTestEnvironment.UNIT, new KafkaAdminServiceUnitTestModule());
    }

    @Override
    protected EnumMap<KafkaAdminTestEnvironment, AbstractModule> provideModules() {
        return testEnvironmentAbstractModuleEnumMap;
    }

    @Override
    protected Map<String, Object> buildProps(Class<?> klass) {
        Map<String, Object> props = new HashMap<>();
        KafkaAdminServiceTest kafkaAdminServiceTestAnnotation = klass.getAnnotation(KafkaAdminServiceTest.class);
        if (kafkaAdminServiceTestAnnotation != null) {
            props.put(KafkaAdminTestConstants.NUM_BROKER_NODES, kafkaAdminServiceTestAnnotation.numBrokerNodes());
        }
        return props;
    }
}