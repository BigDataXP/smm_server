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
package com.hortonworks.smm.kafka.services.security;

import javax.ws.rs.core.SecurityContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Principal;
import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class SecurityUtil {
    private static final Logger LOG = LoggerFactory.getLogger(SecurityUtil.class);


    public static boolean authorizeTopicDescribe(SMMAuthorizer authorizer, SecurityContext securityContext,
                                                 String resourceName) {
        Principal principal = securityContext.getUserPrincipal();
        return authorize(authorizer, principal, ResourceType.TOPIC, resourceName, Permission.DESCRIBE);
    }

    public static boolean authorizeTopicRead(SMMAuthorizer authorizer, SecurityContext securityContext,
                                                 String resourceName) {
        Principal principal = securityContext.getUserPrincipal();
        return authorize(authorizer, principal, ResourceType.TOPIC, resourceName, Permission.READ);
    }

    public static boolean authorizeGroupDescribe(SMMAuthorizer authorizer, SecurityContext securityContext,
                                                 String resourceName) {
        Principal principal = securityContext.getUserPrincipal();
        return authorize(authorizer, principal, ResourceType.GROUP, resourceName, Permission.DESCRIBE);
    }

    public static boolean authorize(SMMAuthorizer authorizer, SecurityContext securityContext,
                                    ResourceType resourceType, String resourceName,
                                    Permission permission) {
        Principal principal = securityContext.getUserPrincipal();
        return authorize(authorizer, principal, resourceType, resourceName, permission);
    }

    public static <T> Collection<T> filterGroups(SMMAuthorizer authorizer, SecurityContext securityContext, Collection<T> entities,
                                           Function<T, String> resourceNameFunction) {
        return filter(authorizer, securityContext, ResourceType.GROUP, entities, resourceNameFunction, Permission.DESCRIBE);
    }

    public static <T> Collection<T> filterTopics(SMMAuthorizer authorizer, SecurityContext securityContext, Collection<T> entities,
                                           Function<T, String> resourceNameFunction) {
        return filter(authorizer, securityContext, ResourceType.TOPIC, entities, resourceNameFunction, Permission.DESCRIBE);
    }

    public static <T> Collection<T> filter(SMMAuthorizer authorizer, SecurityContext securityContext,
                                           ResourceType resourceType, Collection<T> entities,
                                           Function<T, String> resourceNameFunction,
                                           Permission permission) {
        Principal principal = securityContext.getUserPrincipal();
        return entities.stream()
            .filter(e -> authorize(authorizer, principal, resourceType, resourceNameFunction.apply(e), permission))
            .collect(Collectors.toList());
    }

    private static boolean authorize(SMMAuthorizer authorizer, Principal principal,
                                     ResourceType resourceType, String resourceName,
                                     Permission permission) {
        AuthenticationContext authenticationCtx = SecurityUtil.getAuthenticationContext(principal);
        return authorizer.authorize(authenticationCtx, resourceType, resourceName, permission);
    }

    public static String getUserName(String principalName) {
        return principalName == null ? null : principalName.split("[/@]")[0];
    }

    public static String getUserName(AuthenticationContext context) {
        return context.getPrincipal() == null ? null : getUserName(context.getPrincipal().getName());
    }

    private static AuthenticationContext getAuthenticationContext(Principal principal) {
        AuthenticationContext context = new AuthenticationContext();
        context.setPrincipal(principal);
        return context;
    }
}
