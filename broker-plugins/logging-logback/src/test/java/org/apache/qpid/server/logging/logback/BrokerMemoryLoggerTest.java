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
package org.apache.qpid.server.logging.logback;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerLogger;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.JsonSystemConfigImpl;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.store.ConfiguredObjectRecord;
import org.apache.qpid.server.store.GenericRecoverer;
import org.apache.qpid.test.utils.UnitTestBase;

public class BrokerMemoryLoggerTest extends UnitTestBase
{
    private TaskExecutor _taskExecutor;
    private SystemConfig<JsonSystemConfigImpl> _systemConfig;
    private ConfiguredObjectRecord _brokerEntry = mock(ConfiguredObjectRecord.class);
    private UUID _brokerId = UUID.randomUUID();

    @Before
    public void setUp() throws Exception
    {
        _taskExecutor = new CurrentThreadTaskExecutor();
        _taskExecutor.start();
        _systemConfig = new JsonSystemConfigImpl(_taskExecutor,
                                                 mock(EventLogger.class),
                                                 null, new HashMap<String,Object>())
                        {
                            {
                                updateModel(BrokerModel.getInstance());
                            }
                        };


        when(_brokerEntry.getId()).thenReturn(_brokerId);
        when(_brokerEntry.getType()).thenReturn(Broker.class.getSimpleName());
        Map<String, Object> attributesMap = new HashMap<>();
        attributesMap.put(Broker.MODEL_VERSION, BrokerModel.MODEL_VERSION);
        attributesMap.put(Broker.NAME, getTestName());

        when(_brokerEntry.getAttributes()).thenReturn(attributesMap);
        when(_brokerEntry.getParents()).thenReturn(Collections.singletonMap(SystemConfig.class.getSimpleName(), _systemConfig.getId()));
        GenericRecoverer recoverer = new GenericRecoverer(_systemConfig);
        recoverer.recover(Arrays.asList(_brokerEntry), false);
    }

    @Test
    public void testCreateDeleteBrokerMemoryLogger()
    {
        final String brokerLoggerName = "TestBrokerLogger";
        ch.qos.logback.classic.Logger rootLogger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        Broker broker = _systemConfig.getContainer(Broker.class);
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ConfiguredObject.NAME, brokerLoggerName);
        attributes.put(ConfiguredObject.TYPE, BrokerMemoryLogger.TYPE);

        BrokerLogger brokerLogger = (BrokerLogger) broker.createChild(BrokerLogger.class, attributes);
        assertEquals("Created BrokerLogger has unexpected name", brokerLoggerName, brokerLogger.getName());
        final boolean condition = brokerLogger instanceof BrokerMemoryLogger;
        assertTrue("BrokerLogger has unexpected type", condition);

        assertNotNull("Appender not attached to root logger after BrokerLogger creation",
                             rootLogger.getAppender(brokerLoggerName));


        brokerLogger.delete();

        assertNull("Appender should be no longer attached to root logger after BrokerLogger deletion",
                          rootLogger.getAppender(brokerLoggerName));

    }

    @Test
    public void testBrokerMemoryLoggerRestrictsBufferSize()
    {
        doMemoryLoggerLimitsTest(BrokerMemoryLogger.MAX_RECORD_LIMIT + 1, BrokerMemoryLogger.MAX_RECORD_LIMIT);
        doMemoryLoggerLimitsTest(0, 1);
    }

    private void doMemoryLoggerLimitsTest(final int illegalValue, final int legalValue)
    {
        final String brokerLoggerName = "TestBrokerLogger";

        Broker broker = _systemConfig.getContainer(Broker.class);
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ConfiguredObject.NAME, brokerLoggerName);
        attributes.put(ConfiguredObject.TYPE, BrokerMemoryLogger.TYPE);
        attributes.put(BrokerMemoryLogger.MAX_RECORDS, illegalValue);

        try
        {
            broker.createChild(BrokerLogger.class, attributes);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException ice)
        {
            // PASS
        }

        attributes.put(BrokerMemoryLogger.MAX_RECORDS, legalValue);
        BrokerLogger brokerLogger = (BrokerLogger) broker.createChild(BrokerLogger.class, attributes);

        try
        {
            brokerLogger.setAttributes(Collections.singletonMap(BrokerMemoryLogger.MAX_RECORDS, illegalValue));
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException ice)
        {
            // PASS
        }
        finally
        {
            brokerLogger.delete();
        }
    }
}
