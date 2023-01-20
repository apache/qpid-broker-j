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
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

    @BeforeEach
    public void setUp() throws Exception
    {
        _broker = BrokerTestHelper.createNewBrokerMock();
        _objectFactory = _broker.getObjectFactory();
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        if (_passwordFile.exists())
        {
            _passwordFile.delete();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testExistingPasswordFile()
    {
        _passwordFile = TestFileUtils.createTempFile(this, ".user.password", "user:password");

        final Map<String, Object> providerAttrs = Map.of(PlainPasswordDatabaseAuthenticationManager.TYPE, PROVIDER_TYPE,
                PlainPasswordDatabaseAuthenticationManager.PATH, _passwordFile.getAbsolutePath(),
                PlainPasswordDatabaseAuthenticationManager.NAME, getTestName());

        final AuthenticationProvider<?> provider = _objectFactory.create(AuthenticationProvider.class, providerAttrs, _broker);
        assertThat(provider.getChildren(User.class).size(), is(equalTo(1)));

        final User<?> user = (User<?>) provider.getChildByName(User.class, "user");
        assertThat(user.getName(), is(equalTo("user")));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAddUser()
    {
        _passwordFile = TestFileUtils.createTempFile(this, ".user.password");

        final Map<String, Object> providerAttrs = Map.of(PlainPasswordDatabaseAuthenticationManager.TYPE, PROVIDER_TYPE,
                PlainPasswordDatabaseAuthenticationManager.PATH, _passwordFile.getAbsolutePath(),
                PlainPasswordDatabaseAuthenticationManager.NAME, getTestName());

        final AuthenticationProvider<?> provider = _objectFactory.create(AuthenticationProvider.class, providerAttrs, _broker);
        assertThat(provider.getChildren(User.class).size(), is(equalTo(0)));

        final Map<String, Object> userAttrs = Map.of(User.TYPE, PROVIDER_TYPE,
                User.NAME, "user",
                User.PASSWORD, "password");
        final User<?> user = (User<?>) provider.createChild(User.class, userAttrs);

        assertThat(provider.getChildren(User.class).size(), is(equalTo(1)));
        assertThat(user.getName(), is(equalTo("user")));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRemoveUser()
    {
        _passwordFile = TestFileUtils.createTempFile(this, ".user.password", "user:password");

        final Map<String, Object> providerAttrs = Map.of(PlainPasswordDatabaseAuthenticationManager.TYPE, PROVIDER_TYPE,
                PlainPasswordDatabaseAuthenticationManager.PATH, _passwordFile.getAbsolutePath(),
                PlainPasswordDatabaseAuthenticationManager.NAME, getTestName());

        final AuthenticationProvider<?> provider = _objectFactory.create(AuthenticationProvider.class, providerAttrs, _broker);
        assertThat(provider.getChildren(User.class).size(), is(equalTo(1)));

        final User<?> user = (User<?>) provider.getChildByName(User.class, "user");
        user.delete();

        assertThat(provider.getChildren(User.class).size(), is(equalTo(0)));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDurability()
    {
        _passwordFile = TestFileUtils.createTempFile(this, ".user.password");

        final Map<String, Object> providerAttrs = Map.of(PlainPasswordDatabaseAuthenticationManager.TYPE, PROVIDER_TYPE,
                PlainPasswordDatabaseAuthenticationManager.PATH, _passwordFile.getAbsolutePath(),
                PlainPasswordDatabaseAuthenticationManager.NAME, getTestName());

        {
            final AuthenticationProvider<?> provider =
                    _objectFactory.create(AuthenticationProvider.class, providerAttrs, _broker);
            assertThat(provider.getChildren(User.class).size(), is(equalTo(0)));

            final Map<String, Object> userAttrs =Map.of(User.TYPE, PROVIDER_TYPE,
                    User.NAME, "user",
                    User.PASSWORD, "password");
            provider.createChild(User.class, userAttrs);

            provider.close();
        }

        {
            final AuthenticationProvider<?> provider =
                    _objectFactory.create(AuthenticationProvider.class, providerAttrs, _broker);
            assertThat(provider.getChildren(User.class).size(), is(equalTo(1)));

            final User<?> user = (User<?>) provider.getChildByName(User.class, "user");
            user.delete();

            provider.close();
        }

        {
            final AuthenticationProvider<?> provider =
                    _objectFactory.create(AuthenticationProvider.class, providerAttrs, _broker);
            assertThat(provider.getChildren(User.class).size(), is(equalTo(0)));

            provider.close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAuthenticate()
    {
        _passwordFile = TestFileUtils.createTempFile(this, ".user.password", "user:password");

        final String file = _passwordFile.getAbsolutePath();
        final Map<String, Object> providerAttrs = Map.of(PlainPasswordDatabaseAuthenticationManager.TYPE, PROVIDER_TYPE,
                PlainPasswordDatabaseAuthenticationManager.PATH, file,
                PlainPasswordDatabaseAuthenticationManager.NAME, getTestName());

        final PasswordCredentialManagingAuthenticationProvider<?> provider =
                ((PasswordCredentialManagingAuthenticationProvider<?>) _objectFactory.create(AuthenticationProvider.class,
                                                                                          providerAttrs,
                                                                                          _broker));

        {
            final AuthenticationResult result = provider.authenticate("user", "password");
            assertThat(result.getStatus(), is(equalTo(SUCCESS)));
        }

        {
            final AuthenticationResult result = provider.authenticate("user", "badpassword");
            assertThat(result.getStatus(), is(equalTo(AuthenticationResult.AuthenticationStatus.ERROR)));
        }

        {
            final AuthenticationResult result = provider.authenticate("unknownuser", "badpassword");
            assertThat(result.getStatus(), is(equalTo(AuthenticationResult.AuthenticationStatus.ERROR)));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDeleteProvider()
    {
        _passwordFile = TestFileUtils.createTempFile(this, ".user.password", "user:password");

        final Map<String, Object> providerAttrs = Map.of(PlainPasswordDatabaseAuthenticationManager.TYPE, PROVIDER_TYPE,
                PlainPasswordDatabaseAuthenticationManager.PATH, _passwordFile.getAbsolutePath(),
                PlainPasswordDatabaseAuthenticationManager.NAME, getTestName());

        final AuthenticationProvider<?> provider = _objectFactory.create(AuthenticationProvider.class, providerAttrs, _broker);
        provider.delete();

        assertThat(_passwordFile.exists(), is(equalTo(false)));
    }
}