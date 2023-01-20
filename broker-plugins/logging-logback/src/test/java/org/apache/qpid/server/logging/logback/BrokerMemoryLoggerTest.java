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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

@SuppressWarnings({"rawtypes", "unchecked"})
public class BrokerMemoryLoggerTest extends UnitTestBase
{
    private final ConfiguredObjectRecord _brokerEntry = mock(ConfiguredObjectRecord.class);
    private final UUID _brokerId = UUID.randomUUID();
    private TaskExecutor _taskExecutor;
    private SystemConfig<JsonSystemConfigImpl> _systemConfig;

    @BeforeEach
    public void setUp() throws Exception
    {
        _taskExecutor = CurrentThreadTaskExecutor.newStartedInstance();
        _systemConfig = new JsonSystemConfigImpl(_taskExecutor, mock(EventLogger.class), null, new HashMap<>())
        {
            {
                updateModel(BrokerModel.getInstance());
            }
        };


        when(_brokerEntry.getId()).thenReturn(_brokerId);
        when(_brokerEntry.getType()).thenReturn(Broker.class.getSimpleName());
        final Map<String, Object> attributesMap = Map.of(Broker.MODEL_VERSION, BrokerModel.MODEL_VERSION,
                Broker.NAME, getTestName());

        when(_brokerEntry.getAttributes()).thenReturn(attributesMap);
        when(_brokerEntry.getParents()).thenReturn(Collections.singletonMap(SystemConfig.class.getSimpleName(), _systemConfig.getId()));
        final GenericRecoverer recoverer = new GenericRecoverer(_systemConfig);
        recoverer.recover(Collections.singletonList(_brokerEntry), false);
    }

    @Test
    public void testCreateDeleteBrokerMemoryLogger()
    {
        final String brokerLoggerName = "TestBrokerLogger";
        final ch.qos.logback.classic.Logger rootLogger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        final Broker broker = _systemConfig.getContainer(Broker.class);
        final Map<String, Object> attributes = Map.of(ConfiguredObject.NAME, brokerLoggerName,
                ConfiguredObject.TYPE, BrokerMemoryLogger.TYPE);

        final BrokerLogger brokerLogger = (BrokerLogger) broker.createChild(BrokerLogger.class, attributes);
        assertEquals(brokerLoggerName, brokerLogger.getName(), "Created BrokerLogger has unexpected name");
        final boolean condition = brokerLogger instanceof BrokerMemoryLogger;
        assertTrue(condition, "BrokerLogger has unexpected type");

        assertNotNull(rootLogger.getAppender(brokerLoggerName),
                "Appender not attached to root logger after BrokerLogger creation");

        brokerLogger.delete();

        assertNull(rootLogger.getAppender(brokerLoggerName),
                "Appender should be no longer attached to root logger after BrokerLogger deletion");
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

        final Broker broker = _systemConfig.getContainer(Broker.class);
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(ConfiguredObject.NAME, brokerLoggerName);
        attributes.put(ConfiguredObject.TYPE, BrokerMemoryLogger.TYPE);
        attributes.put(BrokerMemoryLogger.MAX_RECORDS, illegalValue);

        assertThrows(IllegalConfigurationException.class,
                () -> broker.createChild(BrokerLogger.class, attributes),
                "Exception not thrown");

        attributes.put(BrokerMemoryLogger.MAX_RECORDS, legalValue);
        final BrokerLogger brokerLogger = (BrokerLogger) broker.createChild(BrokerLogger.class, attributes);

        assertThrows(IllegalConfigurationException.class,
                () -> brokerLogger.setAttributes(Map.of(BrokerMemoryLogger.MAX_RECORDS, illegalValue)),
                "Exception not thrown");
        brokerLogger.delete();
    }
}
