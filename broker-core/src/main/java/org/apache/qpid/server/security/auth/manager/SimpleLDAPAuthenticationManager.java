/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.qpid.server.security.auth.manager;

import java.util.List;

import org.apache.qpid.server.model.DerivedAttribute;
import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.model.ManagedContextDefault;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.TrustStore;

@ManagedObject( category = false,
                type = "SimpleLDAP",
                description = SimpleLDAPAuthenticationManager.CLASS_DESCRIPTION )
public interface SimpleLDAPAuthenticationManager<X extends SimpleLDAPAuthenticationManager<X>>
        extends CachingAuthenticationProvider<X>,
                UsernamePasswordAuthenticationProvider<X>
{
    String CLASS_DESCRIPTION = "Authentication provider that delegates authentication decisions to a Directory"
                               + " supporting the LDAP protocol.";

    String PROVIDER_TYPE = "SimpleLDAP";
    String PROVIDER_URL = "providerUrl";
    String PROVIDER_AUTH_URL = "providerAuthUrl";
    String SEARCH_CONTEXT = "searchContext";
    String LDAP_CONTEXT_FACTORY = "ldapContextFactory";
    String SEARCH_USERNAME = "searchUsername";
    String SEARCH_PASSWORD = "searchPassword";
    String TRUST_STORE = "trustStore";
    String SEARCH_FILTER = "searchFilter";
    String GROUP_SEARCH_CONTEXT = "groupSearchContext";
    String GROUP_SEARCH_FILTER = "groupSearchFilter";
    String AUTHENTICATION_METHOD ="authenticationMethod";
    String LOGIN_CONFIG_SCOPE = "loginConfigScope";
    String LOGIN_CONFIG_SCOPE_DEFAULT = "qpid-broker-j";

    @ManagedAttribute( description = "LDAP server URL", mandatory = true)
    String getProviderUrl();

    @ManagedAttribute( description = "LDAP authentication URL")
    String getProviderAuthUrl();

    @ManagedAttribute( description = "Search context", mandatory = true)
    String getSearchContext();

    @ManagedAttribute( description = "Search filter", mandatory = true)
    String getSearchFilter();

    @ManagedAttribute( description = "Bind without search")
    boolean isBindWithoutSearch();

    @ManagedContextDefault( name = "ldap.context.factory")
    String DEFAULT_LDAP_CONTEXT_FACTORY = "com.sun.jndi.ldap.LdapCtxFactory";

    @ManagedAttribute( description = "LDAP context factory", defaultValue = "${ldap.context.factory}")
    String getLdapContextFactory();

    @ManagedAttribute( description = "Trust store name")
    TrustStore getTrustStore();

    @ManagedAttribute( description = "(Optional) username for authenticated search")
    String getSearchUsername();

    @ManagedAttribute( description = "(Optional) password for authenticated search", secure = true)
    String getSearchPassword();

    @ManagedAttribute( description = "User entry attribute name containing group name user belongs to. ")
    String getGroupAttributeName();

    @ManagedAttribute( description = "Search context to look for role entries")
    String getGroupSearchContext();

    @ManagedAttribute( description = "Group search filter to search for groups in group search context")
    String getGroupSearchFilter();

    @ManagedAttribute( description = "Defines the group search scope. If true the search for group entries is performed in the entire subtree of the group search context")
    boolean isGroupSubtreeSearchScope();

    @ManagedAttribute(description = "Method of authentication to use when binding into LDAP. Supported methods: NONE, SIMPLE, GSSAPI.",
            defaultValue = "NONE")
    LdapAuthenticationMethod getAuthenticationMethod();

    @ManagedAttribute(description = "The scope of JAAS configuration from login module to use to obtain Kerberos"
                                    + " initiator credentials when the authentication method is GSSAPI",
            defaultValue = LOGIN_CONFIG_SCOPE_DEFAULT)
    String getLoginConfigScope();

    @DerivedAttribute
    List<String> getTlsProtocolWhiteList();

    @DerivedAttribute
    List<String> getTlsProtocolBlackList();

    @DerivedAttribute
    List<String> getTlsCipherSuiteWhiteList();

    @DerivedAttribute
    List<String> getTlsCipherSuiteBlackList();

}
