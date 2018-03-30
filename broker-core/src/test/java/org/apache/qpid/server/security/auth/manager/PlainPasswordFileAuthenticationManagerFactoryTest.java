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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.security.auth.database.PlainPasswordFilePrincipalDatabase;
import org.apache.qpid.test.utils.UnitTestBase;

public class PlainPasswordFileAuthenticationManagerFactoryTest extends UnitTestBase
{

    private ConfiguredObjectFactory _factory = BrokerModel.getInstance().getObjectFactory();
    private Map<String, Object> _configuration = new HashMap<String, Object>();
    private File _emptyPasswordFile;
    private Broker _broker = BrokerTestHelper.createBrokerMock();

    @Before
    public void setUp() throws Exception
    {
        _emptyPasswordFile = File.createTempFile(getTestName(), "passwd");
        _emptyPasswordFile.deleteOnExit();
        _configuration.put(AuthenticationProvider.ID, UUID.randomUUID());
        _configuration.put(AuthenticationProvider.NAME, getTestName());
    }

    @Test
    public void testPlainInstanceCreated() throws Exception
    {
        _configuration.put(AuthenticationProvider.TYPE, PlainPasswordDatabaseAuthenticationManager.PROVIDER_TYPE);
        _configuration.put("path", _emptyPasswordFile.getAbsolutePath());

        AuthenticationProvider manager = _factory.create(AuthenticationProvider.class, _configuration, _broker);
        assertNotNull(manager);
        assertTrue(manager instanceof PrincipalDatabaseAuthenticationManager);
        assertTrue(((PrincipalDatabaseAuthenticationManager)manager).getPrincipalDatabase() instanceof PlainPasswordFilePrincipalDatabase);
    }

    @Test
    public void testPasswordFileNotFound() throws Exception
    {
        //delete the file
        _emptyPasswordFile.delete();

        _configuration.put(AuthenticationProvider.TYPE, PlainPasswordDatabaseAuthenticationManager.PROVIDER_TYPE);
        _configuration.put("path", _emptyPasswordFile.getAbsolutePath());


        AuthenticationProvider manager = _factory.create(AuthenticationProvider.class, _configuration, _broker);

        assertNotNull(manager);
        assertTrue(manager instanceof PrincipalDatabaseAuthenticationManager);
        assertTrue(((PrincipalDatabaseAuthenticationManager)manager).getPrincipalDatabase() instanceof PlainPasswordFilePrincipalDatabase);
    }

    @Test
    public void testThrowsExceptionWhenConfigForPlainPDImplementationNoPasswordFileValueSpecified() throws Exception
    {
        _configuration.put(AuthenticationProvider.TYPE, PlainPasswordDatabaseAuthenticationManager.PROVIDER_TYPE);

        try
        {
            AuthenticationProvider manager = _factory.create(AuthenticationProvider.class, _configuration, _broker);
            fail("No authentication manager should be created");
        }
        catch (IllegalArgumentException e)
        {
            // pass;
        }
    }

    @After
    public void tearDown() throws Exception
    {
        if (_emptyPasswordFile == null && _emptyPasswordFile.exists())
        {
            _emptyPasswordFile.delete();
        }
    }
}
