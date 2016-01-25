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

package org.apache.qpid.server.security.auth.manager;

import java.util.HashMap;
import java.util.Map;

import javax.jms.JMSException;
import javax.servlet.http.HttpServletResponse;

import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.User;
import org.apache.qpid.systest.rest.RestTestHelper;
import org.apache.qpid.test.utils.QpidBrokerTestCase;
import org.apache.qpid.test.utils.TestBrokerConfiguration;

public abstract class AbstractAuthenticationManagerTest extends QpidBrokerTestCase
{
    @Override
    protected void setUp() throws Exception
    {
        TestBrokerConfiguration config = getDefaultBrokerConfiguration();
        config.addHttpManagementConfiguration();

        Map<String, Object> authProviderAttrs = getAuthenticationProviderAttributes();
        config.addObjectConfiguration(AuthenticationProvider.class, authProviderAttrs);

        config.setObjectAttribute(Port.class, TestBrokerConfiguration.ENTRY_NAME_AMQP_PORT,
                                  Port.AUTHENTICATION_PROVIDER, getAuthenticationProviderName());

        super.setUp();
    }

    protected abstract String getAuthenticationProviderName();

    protected abstract Map<String, Object> getAuthenticationProviderAttributes();

    public void testConnect_Success() throws Exception
    {
        createUser("myuser", "mypassword");
        getConnection("myuser", "mypassword");
    }

    public void testConnect_WrongPassword() throws Exception
    {
        createUser("myuser", "mypassword");
        try
        {
            getConnection("myuser", "badpassword");
            fail("Exception not thrown");
        }
        catch(JMSException je)
        {
            // PASS
        }
    }

    public void testConnect_UnknownUser() throws Exception
    {
        createUser("myuser", "mypassword");
        try
        {
            getConnection("unknown", "mypassword");
            fail("Exception not thrown");
        }
        catch(JMSException je)
        {
            // PASS
        }
    }

    private void createUser(final String user, final String password) throws Exception
    {
        RestTestHelper restTestHelper = new RestTestHelper(getDefaultBroker().getHttpPort());

        final Map<String, Object> userAttr = new HashMap<>();
        userAttr.put(User.NAME, user);
        userAttr.put(User.PASSWORD, password);

        String url = "user/" + getAuthenticationProviderName();
        restTestHelper.submitRequest(url, "POST", userAttr, HttpServletResponse.SC_CREATED);
    }
}
