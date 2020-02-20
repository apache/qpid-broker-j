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

import static org.apache.qpid.server.security.auth.AuthenticationResult.AuthenticationStatus.SUCCESS;
import static org.apache.qpid.server.security.auth.manager.PlainPasswordDatabaseAuthenticationManager.PROVIDER_TYPE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.PasswordCredentialManagingAuthenticationProvider;
import org.apache.qpid.server.model.User;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.test.utils.TestFileUtils;
import org.apache.qpid.test.utils.UnitTestBase;

public class PlainPasswordDatabaseAuthenticationManagerTest extends UnitTestBase
{
    private Broker<?> _broker;
    private File _passwordFile;
    private ConfiguredObjectFactory _objectFactory;

    @Before
    public void setUp() throws Exception
    {

        _broker = BrokerTestHelper.createBrokerMock();
        _objectFactory = _broker.getObjectFactory();
    }

    @After
    public void tearDown() throws Exception
    {
        try
        {
            if (_passwordFile.exists())
            {
                _passwordFile.delete();
            }
        }
        finally
        {
        }
    }

    @Test
    public void testExistingPasswordFile()
    {
        _passwordFile = TestFileUtils.createTempFile(this, ".user.password", "user:password");

        Map<String, Object> providerAttrs = new HashMap<>();
        providerAttrs.put(PlainPasswordDatabaseAuthenticationManager.TYPE, PROVIDER_TYPE);
        providerAttrs.put(PlainPasswordDatabaseAuthenticationManager.PATH, _passwordFile.getAbsolutePath());
        providerAttrs.put(PlainPasswordDatabaseAuthenticationManager.NAME, getTestName());

        @SuppressWarnings("unchecked")
        AuthenticationProvider provider = _objectFactory.create(AuthenticationProvider.class, providerAttrs, _broker);
        assertThat(provider.getChildren(User.class).size(), is(equalTo(1)));

        User user = (User) provider.getChildByName(User.class, "user");
        assertThat(user.getName(), is(equalTo("user")));
    }

    @Test
    public void testAddUser()
    {
        _passwordFile = TestFileUtils.createTempFile(this, ".user.password");

        Map<String, Object> providerAttrs = new HashMap<>();
        providerAttrs.put(PlainPasswordDatabaseAuthenticationManager.TYPE, PROVIDER_TYPE);
        providerAttrs.put(PlainPasswordDatabaseAuthenticationManager.PATH, _passwordFile.getAbsolutePath());
        providerAttrs.put(PlainPasswordDatabaseAuthenticationManager.NAME, getTestName());

        AuthenticationProvider provider = _objectFactory.create(AuthenticationProvider.class, providerAttrs, _broker);
        assertThat(provider.getChildren(User.class).size(), is(equalTo(0)));

        Map<String, Object> userAttrs = new HashMap<>();
        userAttrs.put(User.TYPE, PROVIDER_TYPE);
        userAttrs.put(User.NAME, "user");
        userAttrs.put(User.PASSWORD, "password");
        User user = (User) provider.createChild(User.class, userAttrs);

        assertThat(provider.getChildren(User.class).size(), is(equalTo(1)));
        assertThat(user.getName(), is(equalTo("user")));
    }

    @Test
    public void testRemoveUser()
    {
        _passwordFile = TestFileUtils.createTempFile(this, ".user.password", "user:password");

        Map<String, Object> providerAttrs = new HashMap<>();
        providerAttrs.put(PlainPasswordDatabaseAuthenticationManager.TYPE, PROVIDER_TYPE);
        providerAttrs.put(PlainPasswordDatabaseAuthenticationManager.PATH, _passwordFile.getAbsolutePath());
        providerAttrs.put(PlainPasswordDatabaseAuthenticationManager.NAME, getTestName());

        AuthenticationProvider provider = _objectFactory.create(AuthenticationProvider.class, providerAttrs, _broker);
        assertThat(provider.getChildren(User.class).size(), is(equalTo(1)));

        User user = (User) provider.getChildByName(User.class, "user");
        user.delete();

        assertThat(provider.getChildren(User.class).size(), is(equalTo(0)));
    }

    @Test
    public void testDurability()
    {
        _passwordFile = TestFileUtils.createTempFile(this, ".user.password");

        Map<String, Object> providerAttrs = new HashMap<>();
        providerAttrs.put(PlainPasswordDatabaseAuthenticationManager.TYPE, PROVIDER_TYPE);
        providerAttrs.put(PlainPasswordDatabaseAuthenticationManager.PATH, _passwordFile.getAbsolutePath());
        providerAttrs.put(PlainPasswordDatabaseAuthenticationManager.NAME, getTestName());

        {
            AuthenticationProvider provider =
                    _objectFactory.create(AuthenticationProvider.class, providerAttrs, _broker);
            assertThat(provider.getChildren(User.class).size(), is(equalTo(0)));

            Map<String, Object> userAttrs = new HashMap<>();
            userAttrs.put(User.TYPE, PROVIDER_TYPE);
            userAttrs.put(User.NAME, "user");
            userAttrs.put(User.PASSWORD, "password");
            provider.createChild(User.class, userAttrs);

            provider.close();
        }

        {
            AuthenticationProvider provider =
                    _objectFactory.create(AuthenticationProvider.class, providerAttrs, _broker);
            assertThat(provider.getChildren(User.class).size(), is(equalTo(1)));

            User user = (User) provider.getChildByName(User.class, "user");
            user.delete();

            provider.close();
        }

        {
            AuthenticationProvider provider =
                    _objectFactory.create(AuthenticationProvider.class, providerAttrs, _broker);
            assertThat(provider.getChildren(User.class).size(), is(equalTo(0)));

            provider.close();
        }
    }

    @Test
    public void testAuthenticate()
    {
        _passwordFile = TestFileUtils.createTempFile(this, ".user.password", "user:password");

        String file = _passwordFile.getAbsolutePath();
        Map<String, Object> providerAttrs = new HashMap<>();
        providerAttrs.put(PlainPasswordDatabaseAuthenticationManager.TYPE, PROVIDER_TYPE);
        providerAttrs.put(PlainPasswordDatabaseAuthenticationManager.PATH, file);
        providerAttrs.put(PlainPasswordDatabaseAuthenticationManager.NAME, getTestName());

        PasswordCredentialManagingAuthenticationProvider provider =
                ((PasswordCredentialManagingAuthenticationProvider) _objectFactory.create(AuthenticationProvider.class,
                                                                                          providerAttrs,
                                                                                          _broker));

        {
            AuthenticationResult result = provider.authenticate("user", "password");
            assertThat(result.getStatus(), is(equalTo(SUCCESS)));
        }

        {
            AuthenticationResult result = provider.authenticate("user", "badpassword");
            assertThat(result.getStatus(), is(equalTo(AuthenticationResult.AuthenticationStatus.ERROR)));
        }

        {
            AuthenticationResult result = provider.authenticate("unknownuser", "badpassword");
            assertThat(result.getStatus(), is(equalTo(AuthenticationResult.AuthenticationStatus.ERROR)));
        }
    }

    @Test
    public void testDeleteProvider()
    {
        _passwordFile = TestFileUtils.createTempFile(this, ".user.password", "user:password");

        Map<String, Object> providerAttrs = new HashMap<>();
        providerAttrs.put(PlainPasswordDatabaseAuthenticationManager.TYPE, PROVIDER_TYPE);
        providerAttrs.put(PlainPasswordDatabaseAuthenticationManager.PATH, _passwordFile.getAbsolutePath());
        providerAttrs.put(PlainPasswordDatabaseAuthenticationManager.NAME, getTestName());

        AuthenticationProvider provider = _objectFactory.create(AuthenticationProvider.class, providerAttrs, _broker);
        provider.delete();

        assertThat(_passwordFile.exists(), is(equalTo(false)));
    }
}