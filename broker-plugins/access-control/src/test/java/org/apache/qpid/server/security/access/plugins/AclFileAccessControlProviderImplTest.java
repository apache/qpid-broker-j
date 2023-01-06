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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.test.utils.UnitTestBase;

public class AclFileAccessControlProviderImplTest extends UnitTestBase
{
    private TaskExecutor _taskExecutor;
    private Model _model;
    private Broker _broker;

    @BeforeEach
    public void setUp() throws Exception
    {
        _taskExecutor = CurrentThreadTaskExecutor.newStartedInstance();
        _model = BrokerModel.getInstance();

        _broker = mock(Broker.class);
        when(_broker.getTaskExecutor()).thenReturn(_taskExecutor);
        when(_broker.getChildExecutor()).thenReturn(_taskExecutor);
        when(_broker.getModel()).thenReturn(_model);
        when(_broker.getId()).thenReturn(UUID.randomUUID());
        when(_broker.getEventLogger()).thenReturn(new EventLogger());
    }

    @Test
    public void testValidationOnCreateWithNonExistingACLFile()
    {
        Map<String,Object> attributes = new HashMap<>();
        String aclFilePath = new File(TMP_FOLDER, "test_" + getTestName() + System.nanoTime() + ".acl").getAbsolutePath();

        attributes.put("path", aclFilePath);
        attributes.put(AclFileAccessControlProvider.NAME, getTestName());


        AclFileAccessControlProviderImpl aclProvider = new AclFileAccessControlProviderImpl(attributes, _broker);
        try
        {
            aclProvider.create();
            fail("Exception is expected on validation with non-existing ACL file");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals(String.format("Cannot convert %s to a readable resource", aclFilePath), e.getMessage(),
                    "Unexpected exception message:" + e.getMessage());

        }
    }

}
