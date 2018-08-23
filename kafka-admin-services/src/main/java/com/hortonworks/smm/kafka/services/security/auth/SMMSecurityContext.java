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
package com.hortonworks.smm.kafka.services.security.auth;

import javax.ws.rs.core.SecurityContext;

import java.security.Principal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds authenticated principal and security context which gets passed to the JAX-RS request methods
 */
public class SMMSecurityContext implements SecurityContext {
    private static final Logger LOG = LoggerFactory.getLogger(SMMSecurityContext.class);

    private final Principal principal;
    private final String scheme;
    private final String authenticationScheme;

    public SMMSecurityContext(Principal principal, String scheme) {
        this(principal, scheme, SecurityContext.BASIC_AUTH);
    }

    public SMMSecurityContext(Principal principal, String scheme, String authenticationScheme) {
        this.principal = principal;
        this.scheme = scheme;
        this.authenticationScheme = authenticationScheme;
    }

    @Override
    public Principal getUserPrincipal() {
        return principal;
    }

    @Override
    public boolean isUserInRole(String role) {
        LOG.debug("isUserInRole user: {}, role: {}", principal, role);
        return false;
    }

    @Override
    public boolean isSecure() {
        return "https".equals(this.scheme);
    }

    @Override
    public String getAuthenticationScheme() {
        return authenticationScheme;
    }

    @Override
    public String toString() {
        return "SMMSecurityContext{" +
                "principal=" + principal +
                ", scheme='" + scheme + '\'' +
                ", authenticationScheme='" + authenticationScheme + '\'' +
                ", isSecure=" + isSecure() +
                '}';
    }
}