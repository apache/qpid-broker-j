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
package org.apache.qpid.server.model;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import ch.qos.logback.core.Appender;
import org.apache.qpid.server.BrokerOptions;
import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.BrokerMemoryLogger;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.store.ConfiguredObjectRecord;
import org.apache.qpid.server.store.GenericRecoverer;
import org.apache.qpid.test.utils.QpidTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerTest extends QpidTestCase
{
    private TaskExecutor _taskExecutor;
    private SystemConfig<JsonSystemConfigImpl> _systemConfig;
    private ConfiguredObjectRecord _brokerEntry = mock(ConfiguredObjectRecord.class);
    private UUID _brokerId = UUID.randomUUID();

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _taskExecutor = new CurrentThreadTaskExecutor();
        _taskExecutor.start();
        _systemConfig = new JsonSystemConfigImpl(_taskExecutor,
                                                 mock(EventLogger.class),
                                                 new BrokerOptions().convertToSystemConfigAttributes());

        when(_brokerEntry.getId()).thenReturn(_brokerId);
        when(_brokerEntry.getType()).thenReturn(Broker.class.getSimpleName());
        Map<String, Object> attributesMap = new HashMap<String, Object>();
        attributesMap.put(Broker.MODEL_VERSION, BrokerModel.MODEL_VERSION);
        attributesMap.put(Broker.NAME, getName());

        when(_brokerEntry.getAttributes()).thenReturn(attributesMap);
        when(_brokerEntry.getParents()).thenReturn(Collections.singletonMap(SystemConfig.class.getSimpleName(), _systemConfig.getId()));
        GenericRecoverer recoverer = new GenericRecoverer(_systemConfig);
        recoverer.recover(Arrays.asList(_brokerEntry));
    }

    public void testCreateBrokerLogger()
    {
        Broker broker = _systemConfig.getBroker();
        Map<String,Object> attributes = new HashMap<>();
        final String brokerLoggerName = "TestBrokerLogger";
        attributes.put(ConfiguredObject.NAME, brokerLoggerName);
        attributes.put(ConfiguredObject.TYPE, BrokerMemoryLogger.TYPE);

        BrokerLogger child = (BrokerLogger) broker.createChild(BrokerLogger.class, attributes);
        assertEquals("Created BrokerLoger has unexcpected name", brokerLoggerName, child.getName());
        assertTrue("BrokerLogger has unexpected type", child instanceof BrokerMemoryLogger);

        ch.qos.logback.classic.Logger rootLogger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        Appender appender = rootLogger.getAppender(brokerLoggerName);
        assertNotNull("Appender not attached to root logger after BrokerLogger creation", appender);
    }
}
