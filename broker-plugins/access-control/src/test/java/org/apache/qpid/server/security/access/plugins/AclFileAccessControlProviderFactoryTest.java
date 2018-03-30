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
package org.apache.qpid.server.security.access.plugins;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.AccessControlProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObjectFactoryImpl;
import org.apache.qpid.test.utils.TestFileUtils;
import org.apache.qpid.test.utils.UnitTestBase;

public class AclFileAccessControlProviderFactoryTest extends UnitTestBase
{
    private Broker _broker;
    private ConfiguredObjectFactoryImpl _objectFactory;

    @Before
    public void setUp() throws Exception
    {
        _broker = mock(Broker.class);
        _objectFactory = new ConfiguredObjectFactoryImpl(BrokerModel.getInstance());

        when(_broker.getObjectFactory()).thenReturn(_objectFactory);
        when(_broker.getModel()).thenReturn(_objectFactory.getModel());
        when(_broker.getCategoryClass()).thenReturn(Broker.class);
        TaskExecutor taskExecutor = new CurrentThreadTaskExecutor();
        taskExecutor.start();
        when(_broker.getTaskExecutor()).thenReturn(taskExecutor);
        when(_broker.getChildExecutor()).thenReturn(taskExecutor);
        when(_broker.getEventLogger()).thenReturn(new EventLogger());

    }

    @Test
    public void testCreateInstanceWhenAclFileIsNotPresent()
    {
        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(AccessControlProvider.ID, UUID.randomUUID());
        attributes.put(AccessControlProvider.NAME, "acl");
        attributes.put(AccessControlProvider.TYPE, AclFileAccessControlProvider.ACL_FILE_PROVIDER_TYPE);

        try
        {
            AccessControlProvider acl = _objectFactory.create(AccessControlProvider.class, attributes, _broker);
            fail("ACL was created without a configuration file path specified");
        }
        catch(IllegalArgumentException e)
        {
            // pass
        }
    }


    @Test
    public void testCreateInstanceWhenAclFileIsSpecified()
    {
        File aclFile = TestFileUtils.createTempFile(this, ".acl", "ACL ALLOW all all");
        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(AccessControlProvider.ID, UUID.randomUUID());
        attributes.put(AccessControlProvider.NAME, "acl");
        attributes.put(AccessControlProvider.TYPE, AclFileAccessControlProvider.ACL_FILE_PROVIDER_TYPE);
        attributes.put(AclFileAccessControlProvider.PATH, aclFile.getAbsolutePath());
        AccessControlProvider acl = _objectFactory.create(AccessControlProvider.class, attributes, _broker);

        assertNotNull("ACL was not created from acl file: " + aclFile.getAbsolutePath(), acl);
    }

    @Test
    public void testCreateInstanceWhenAclFileIsSpecifiedButDoesNotExist()
    {
        File aclFile = new File(TMP_FOLDER, "my-non-existing-acl-" + System.currentTimeMillis());
        assertFalse("ACL file " + aclFile.getAbsolutePath() + " actually exists but should not",
                           aclFile.exists());

        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(AccessControlProvider.ID, UUID.randomUUID());
        attributes.put(AccessControlProvider.NAME, "acl");
        attributes.put(AccessControlProvider.TYPE, AclFileAccessControlProvider.ACL_FILE_PROVIDER_TYPE);
        attributes.put(AclFileAccessControlProvider.PATH, aclFile.getAbsolutePath());
        try
        {
            AccessControlProvider control = _objectFactory.create(AccessControlProvider.class, attributes, _broker);
            fail("It should not be possible to create and initialise ACL with non existing file");
        }
        catch (IllegalConfigurationException e)
        {
            assertTrue("Unexpected exception message: " + e.getMessage(),
                              Pattern.matches("Cannot convert .* to a readable resource", e.getMessage()));

        }
    }
}
