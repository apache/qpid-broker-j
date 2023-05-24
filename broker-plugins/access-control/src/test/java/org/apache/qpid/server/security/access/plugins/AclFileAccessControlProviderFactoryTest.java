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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

@SuppressWarnings({"rawtypes", "unchecked"})
class AclFileAccessControlProviderFactoryTest extends UnitTestBase
{
    private Broker _broker;
    private ConfiguredObjectFactoryImpl _objectFactory;

    @BeforeEach
    void setUp()
    {
        _broker = mock(Broker.class);
        _objectFactory = new ConfiguredObjectFactoryImpl(BrokerModel.getInstance());

        when(_broker.getObjectFactory()).thenReturn(_objectFactory);
        when(_broker.getModel()).thenReturn(_objectFactory.getModel());
        when(_broker.getCategoryClass()).thenReturn(Broker.class);
        final TaskExecutor taskExecutor = CurrentThreadTaskExecutor.newStartedInstance();
        when(_broker.getTaskExecutor()).thenReturn(taskExecutor);
        when(_broker.getChildExecutor()).thenReturn(taskExecutor);
        when(_broker.getEventLogger()).thenReturn(new EventLogger());
    }

    @Test
    void createInstanceWhenAclFileIsNotPresent()
    {
        final Map<String, Object> attributes = Map.of(AccessControlProvider.ID, UUID.randomUUID(),
                AccessControlProvider.NAME, "acl",
                AccessControlProvider.TYPE, AclFileAccessControlProvider.ACL_FILE_PROVIDER_TYPE);

        assertThrows(IllegalArgumentException.class,
                () -> _objectFactory.create(AccessControlProvider.class, attributes, _broker),
                "ACL was created without a configuration file path specified");
    }

    @Test
    void createInstanceWhenAclFileIsSpecified()
    {
        final File aclFile = TestFileUtils.createTempFile(this, ".acl", "ACL ALLOW all all");
        final Map<String, Object> attributes = Map.of(AccessControlProvider.ID, UUID.randomUUID(),
                AccessControlProvider.NAME, "acl",
                AccessControlProvider.TYPE, AclFileAccessControlProvider.ACL_FILE_PROVIDER_TYPE,
                AclFileAccessControlProvider.PATH, aclFile.getAbsolutePath());
        final AccessControlProvider acl = _objectFactory.create(AccessControlProvider.class, attributes, _broker);

        assertNotNull(acl, "ACL was not created from acl file: " + aclFile.getAbsolutePath());
    }

    @Test
    void createInstanceWhenAclFileIsSpecifiedButDoesNotExist()
    {
        final File aclFile = new File(TMP_FOLDER, "my-non-existing-acl-" + System.currentTimeMillis());
        assertFalse(aclFile.exists(), "ACL file " + aclFile.getAbsolutePath() + " actually exists but should not");

        final Map<String, Object> attributes = Map.of(AccessControlProvider.ID, UUID.randomUUID(),
                AccessControlProvider.NAME, "acl",
                AccessControlProvider.TYPE, AclFileAccessControlProvider.ACL_FILE_PROVIDER_TYPE,
                AclFileAccessControlProvider.PATH, aclFile.getAbsolutePath());
        final IllegalConfigurationException thrown = assertThrows(IllegalConfigurationException.class,
                () -> _objectFactory.create(AccessControlProvider.class, attributes, _broker),
                "It should not be possible to create and initialise ACL with non existing file");
        assertTrue(Pattern.matches("Cannot convert .* to a readable resource", thrown.getMessage()),
                "Unexpected exception message: " + thrown.getMessage());
    }
}
