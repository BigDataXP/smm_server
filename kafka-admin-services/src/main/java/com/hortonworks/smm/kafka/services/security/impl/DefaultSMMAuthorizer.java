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
package com.hortonworks.smm.kafka.services.security.impl;

import com.hortonworks.smm.kafka.services.security.AuthenticationContext;
import com.hortonworks.smm.kafka.services.security.AuthorizerConfiguration;
import com.hortonworks.smm.kafka.services.security.KafkaAuthorizerConfiguration;
import com.hortonworks.smm.kafka.services.security.Permission;
import com.hortonworks.smm.kafka.services.security.ResourceType;
import com.hortonworks.smm.kafka.services.security.SMMAuthorizer;
import com.hortonworks.smm.kafka.services.security.SecurityUtil;
import kafka.network.RequestChannel;
import kafka.security.auth.Authorizer;
import kafka.security.auth.Operation$;
import kafka.security.auth.Resource;
import kafka.security.auth.ResourceType$;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class DefaultSMMAuthorizer implements SMMAuthorizer {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultSMMAuthorizer.class);

    private Authorizer kafkaAuthorizer;
    private InetAddress clientAddress = getLocalInetAddress();


    @Override
    public void init(AuthorizerConfiguration config) {
        initKafkaAuthorizer(config);
    }

    private void initKafkaAuthorizer(AuthorizerConfiguration authorizerConfiguration) {
        try {
            LOG.info("AuthorizerConfiguration : {}", authorizerConfiguration);
            KafkaAuthorizerConfiguration kafkaAuthorizerConfiguration = authorizerConfiguration.getKafkaAuthorizerConfiguration();
            String kafkaAuthorizerClass = (kafkaAuthorizerConfiguration == null) ? "" : kafkaAuthorizerConfiguration.getClassName();
            if (!StringUtils.isEmpty(kafkaAuthorizerClass)) {
                kafkaAuthorizer = ((Class<Authorizer>) Class.forName(kafkaAuthorizerClass)).newInstance();

                Map<String, Object> configs = kafkaAuthorizerConfiguration.getProperties();
                if (configs == null) {
                    configs = new HashMap<>();
                }

                prepareKafkaConfigs(configs);
                kafkaAuthorizer.configure(configs);

            } else {
                throw new IllegalArgumentException("kafkaAuthorizerClass cannot be empty");
            }
        } catch (Exception e) {
            LOG.error("Error while instantiating DefaultSMMAuthorizer", e);
            throw new IllegalStateException("Error while instantiating DefaultSMMAuthorizer");
        }
    }

    private void prepareKafkaConfigs(Map<String, Object> configs) {
        configs.putIfAbsent(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR, SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_WINDOW_FACTOR);
        configs.putIfAbsent(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER, SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_JITTER);
        configs.putIfAbsent(SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN, SaslConfigs.DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN);
        configs.putIfAbsent(SaslConfigs.SASL_KERBEROS_KINIT_CMD, SaslConfigs.DEFAULT_KERBEROS_KINIT_CMD);
    }


    private InetAddress getLocalInetAddress() {
        try {
            return InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            LOG.error("Error while getting LocalIpAddress", e);
        }
        return null;
    }


    @Override
    public boolean authorize(AuthenticationContext ctx, ResourceType resourceType, String resourceName, Permission permission) {
        try {
            LOG.debug("Received authorize request for : ctx {}, resourceType {}, resourceName {}, permission {}", ctx,
                resourceType, resourceName, permission);
            KafkaPrincipal kafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, SecurityUtil.getUserName(ctx));
            RequestChannel.Session session = new RequestChannel.Session(kafkaPrincipal, clientAddress);
            Resource resource = new Resource(ResourceType$.MODULE$.fromString(resourceType.name()), resourceName);
            return kafkaAuthorizer.authorize(session, Operation$.MODULE$.fromString(permission.name()), resource);
        } catch (Exception e) {
            LOG.error("Error while authorizing request for : ctx {}, resourceType {}, resourceName {}, permission {}", ctx,
                resourceType, resourceName, permission, e);
            return false;
        }
    }

}
