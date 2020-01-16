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
 */

package org.apache.qpid.server.security;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.test.utils.UnitTestBase;

import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class KeyStoreTestBase extends UnitTestBase
{
    protected final TaskExecutor _taskExecutor = CurrentThreadTaskExecutor.newStartedInstance();
    protected final Broker _broker = BrokerTestHelper.createBrokerMock();
    protected final ConfiguredObjectFactory _factory = BrokerModel.getInstance().getObjectFactory();
    protected final Class _keystoreClass;

    protected KeyStoreTestBase(Class keystoreClass)
    {
        _keystoreClass = keystoreClass;
    }

    protected void checkExceptionThrownDuringKeyStoreCreation(Map<String, Object> attributes, String expectedExceptionMessage)
    {
        try
        {
            _factory.create(_keystoreClass, attributes, _broker);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException e)
        {
            final String message = e.getMessage();
            assertTrue("Exception text not as expected:" + message,
                    message.contains(expectedExceptionMessage));

        }
    }
}
