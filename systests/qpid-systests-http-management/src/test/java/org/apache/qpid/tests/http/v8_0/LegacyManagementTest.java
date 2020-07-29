/*
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

package org.apache.qpid.tests.http.v8_0;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import org.junit.Test;

import org.apache.qpid.tests.http.HttpRequestConfig;
import org.apache.qpid.tests.http.HttpTestBase;

@HttpRequestConfig(useVirtualHostAsHost = false)
public class LegacyManagementTest extends HttpTestBase
{
    @Test
    public void testModelVersion() throws Exception
    {
        final Map<String, Object> brokerAttributes = getHelper().getJsonAsMap("/api/v8.0/broker");
        assertThat(brokerAttributes, is(notNullValue()));
        assertThat(brokerAttributes.get("modelVersion"), equalTo("8.0"));
    }

    @Test
    public void testPortAllowDenyProtocolSettings() throws Exception
    {
        final String authenticationProviderName = getTestName() + "AuthenticationProvider";
        final Map<String, Object> authenticationProviderAttributes = new HashMap<>();
        authenticationProviderAttributes.put("type", "Anonymous");
        authenticationProviderAttributes.put("name", authenticationProviderName);
        getHelper().submitRequest("/api/v8.0/authenticationprovider", "POST", authenticationProviderAttributes, HttpServletResponse.SC_CREATED);

        final Map<String, String> context = new HashMap<>();
        context.put("qpid.security.tls.protocolWhiteList", "TLSv1");
        context.put("qpid.security.tls.protocolBlackList", "");
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put("name", getTestName());
        attributes.put("type", "AMQP");
        attributes.put("port", 0);
        attributes.put("context", context);
        attributes.put("authenticationProvider", authenticationProviderName);
        getHelper().submitRequest("/api/v8.0/port", "POST", attributes, HttpServletResponse.SC_CREATED);

        final Map<String, Object> portAttributes = getHelper().getJsonAsMap("port/" + getTestName());
        assertThat(portAttributes, is(notNullValue()));
        final Object portContext = portAttributes.get("context");
        assertThat(portContext, instanceOf(Map.class));
        final Map contextMap = (Map)portContext;
        assertThat(contextMap.get("qpid.security.tls.protocolAllowList"), is(equalTo("TLSv1")));
        assertThat(contextMap.get("qpid.security.tls.protocolDenyList"), is(equalTo("")));

        final Map<String, Object> portAttributes8_0 = getHelper().getJsonAsMap("/api/v8.0/port/" + getTestName());
        assertThat(portAttributes8_0, is(notNullValue()));
        final Object portContext8_0 = portAttributes8_0.get("context");
        assertThat(portContext8_0, instanceOf(Map.class));
        final Map contextMap8_0 = (Map)portContext8_0;
        assertThat(contextMap8_0.get("qpid.security.tls.protocolWhiteList"), is(equalTo("TLSv1")));
        assertThat(contextMap8_0.get("qpid.security.tls.protocolBlackList"), is(equalTo("")));
    }

    @Test
    public void testBrokerAllowDenyProtocolSettings() throws Exception
    {
        final Map<String, String> context = new HashMap<>();
        context.put("qpid.security.tls.protocolWhiteList", "TLSv1");
        context.put("qpid.security.tls.protocolBlackList", "");
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put("context", context);
        getHelper().submitRequest("/api/v8.0/broker", "POST", attributes, HttpServletResponse.SC_OK);

        final Map<String, Object> brokerAttributes = getHelper().getJsonAsMap("broker");
        assertThat(brokerAttributes, is(notNullValue()));
        final Object portContext = brokerAttributes.get("context");
        assertThat(portContext, instanceOf(Map.class));
        final Map contextMap = (Map)portContext;
        assertThat(contextMap.get("qpid.security.tls.protocolAllowList"), is(equalTo("TLSv1")));
        assertThat(contextMap.get("qpid.security.tls.protocolDenyList"), is(equalTo("")));

        final Map<String, Object> brokerAttributes8_0 = getHelper().getJsonAsMap("/api/v8.0/broker");
        assertThat(brokerAttributes8_0, is(notNullValue()));
        final Object brokerContext8_0 = brokerAttributes8_0.get("context");
        assertThat(brokerContext8_0, instanceOf(Map.class));
        final Map contextMap8_0 = (Map)brokerContext8_0;
        assertThat(contextMap8_0.get("qpid.security.tls.protocolWhiteList"), is(equalTo("TLSv1")));
        assertThat(contextMap8_0.get("qpid.security.tls.protocolBlackList"), is(equalTo("")));
    }
}
