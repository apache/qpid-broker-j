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
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.ConfiguredObjectFactoryImpl;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.PasswordCredentialManagingAuthenticationProvider;
import org.apache.qpid.server.model.User;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.test.utils.QpidTestCase;
import org.apache.qpid.test.utils.TestFileUtils;

public class PlainPasswordDatabaseAuthenticationManagerTest extends QpidTestCase
{
    private TaskExecutor _taskExecutor;
    private Broker<?> _broker;
    private File _passwordFile;
    private ConfiguredObjectFactory _objectFactory;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _taskExecutor = CurrentThreadTaskExecutor.newStartedInstance();

        final Model model = BrokerModel.getInstance();
        _objectFactory = new ConfiguredObjectFactoryImpl(model);

        _broker = mock(Broker.class);
        when(_broker.getTaskExecutor()).thenReturn(_taskExecutor);
        when(_broker.getChildExecutor()).thenReturn(_taskExecutor);
        when(_broker.getModel()).thenReturn(model);
        when(_broker.getEventLogger()).thenReturn(mock(EventLogger.class));
    }

    @Override
    public void tearDown() throws Exception
    {
        try
        {
            if (_passwordFile.exists())
            {
                _passwordFile.delete();
            }
            _taskExecutor.stop();
        }
        finally
        {
            super.tearDown();
        }
    }

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