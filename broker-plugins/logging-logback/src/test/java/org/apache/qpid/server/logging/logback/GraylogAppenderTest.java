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

package org.apache.qpid.server.logging.logback;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.Context;
import de.siegmar.logbackgelf.GelfEncoder;
import de.siegmar.logbackgelf.GelfTcpAppender;
import org.apache.qpid.server.logging.logback.event.TestLoggingEvent;
import org.apache.qpid.test.utils.UnitTestBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GraylogAppenderTest extends UnitTestBase
{
    static class TestGelfAppenderConfiguration implements GelfAppenderConfiguration
    {
        private String _remoteHost = "localhost";

        private int _port = 12201;

        private int _reconnectionInterval = 10000;

        private int _connectionTimeout = 300;

        private int _maximumReconnectionAttempts = 1;

        private int _retryDelay = 500;

        private int _messagesFlushTimeOut = 10000;

        private int _messageBufferCapacity = 10000;

        private String _messageOriginHost = "BrokerJ";

        private boolean _rawMessageIncluded = false;

        private boolean _eventMarkerIncluded = false;

        private boolean _mdcPropertiesIncluded = false;

        private boolean _callerDataIncluded = false;

        private boolean _rootExceptionDataIncluded = false;

        private boolean _logLevelNameIncluded = false;

        private final Map<String, Object> _staticFields = new LinkedHashMap<>();

        public TestGelfAppenderConfiguration()
        {
            super();
        }

        @Override
        public String getRemoteHost()
        {
            return _remoteHost;
        }

        public TestGelfAppenderConfiguration withRemoteHost(String remoteHost)
        {
            this._remoteHost = remoteHost;
            return this;
        }

        @Override
        public int getPort()
        {
            return _port;
        }

        public TestGelfAppenderConfiguration withPort(int port)
        {
            this._port = port;
            return this;
        }

        @Override
        public int getReconnectionInterval()
        {
            return _reconnectionInterval;
        }

        public TestGelfAppenderConfiguration withReconnectionInterval(int reconnectionInterval)
        {
            this._reconnectionInterval = reconnectionInterval;
            return this;
        }

        @Override
        public int getConnectionTimeout()
        {
            return _connectionTimeout;
        }

        public TestGelfAppenderConfiguration withConnectionTimeout(int connectionTimeout)
        {
            this._connectionTimeout = connectionTimeout;
            return this;
        }

        @Override
        public int getMaximumReconnectionAttempts()
        {
            return _maximumReconnectionAttempts;
        }

        public TestGelfAppenderConfiguration withMaximumReconnectionAttempts(int maximumReconnectionAttempts)
        {
            this._maximumReconnectionAttempts = maximumReconnectionAttempts;
            return this;
        }

        @Override
        public int getRetryDelay()
        {
            return _retryDelay;
        }

        public TestGelfAppenderConfiguration withRetryDelay(int retryDelay)
        {
            this._retryDelay = retryDelay;
            return this;
        }

        @Override
        public int getMessagesFlushTimeOut()
        {
            return _messagesFlushTimeOut;
        }

        public TestGelfAppenderConfiguration withMessagesFlushTimeOut(int messagesFlushTimeOut)
        {
            this._messagesFlushTimeOut = messagesFlushTimeOut;
            return this;
        }

        @Override
        public int getMessageBufferCapacity()
        {
            return _messageBufferCapacity;
        }

        public TestGelfAppenderConfiguration withMessageBufferCapacity(int messageBufferCapacity)
        {
            this._messageBufferCapacity = messageBufferCapacity;
            return this;
        }

        @Override
        public String getMessageOriginHost()
        {
            return _messageOriginHost;
        }

        public TestGelfAppenderConfiguration withMessageOriginHost(String messageOriginHost)
        {
            this._messageOriginHost = messageOriginHost;
            return this;
        }

        @Override
        public boolean isRawMessageIncluded()
        {
            return _rawMessageIncluded;
        }

        public TestGelfAppenderConfiguration withRawMessageIncluded(boolean rawMessageIncluded)
        {
            this._rawMessageIncluded = rawMessageIncluded;
            return this;
        }

        @Override
        public boolean isEventMarkerIncluded()
        {
            return _eventMarkerIncluded;
        }

        public TestGelfAppenderConfiguration withEventMarkerIncluded(boolean eventMarkerIncluded)
        {
            this._eventMarkerIncluded = eventMarkerIncluded;
            return this;
        }

        @Override
        public boolean hasMdcPropertiesIncluded()
        {
            return _mdcPropertiesIncluded;
        }

        public TestGelfAppenderConfiguration withMdcPropertiesIncluded(boolean mdcPropertiesIncluded)
        {
            this._mdcPropertiesIncluded = mdcPropertiesIncluded;
            return this;
        }

        @Override
        public boolean isCallerDataIncluded()
        {
            return _callerDataIncluded;
        }

        public TestGelfAppenderConfiguration withCallerDataIncluded(boolean callerDataIncluded)
        {
            this._callerDataIncluded = callerDataIncluded;
            return this;
        }

        @Override
        public boolean hasRootExceptionDataIncluded()
        {
            return _rootExceptionDataIncluded;
        }

        public TestGelfAppenderConfiguration withRootExceptionDataIncluded(boolean rootExceptionDataIncluded)
        {
            this._rootExceptionDataIncluded = rootExceptionDataIncluded;
            return this;
        }

        @Override
        public boolean isLogLevelNameIncluded()
        {
            return _logLevelNameIncluded;
        }

        public TestGelfAppenderConfiguration withLogLevelNameIncluded(boolean logLevelNameIncluded)
        {
            this._logLevelNameIncluded = logLevelNameIncluded;
            return this;
        }

        @Override
        public Map<String, Object> getStaticFields()
        {
            return _staticFields;
        }

        public TestGelfAppenderConfiguration addStaticFields(Map<String, ?> map)
        {
            this._staticFields.putAll(map);
            return this;
        }

        public GraylogAppender newAppender(Context context)
        {
            return GraylogAppender.newInstance(context, this);
        }
    }

    static class DefaultGelfAppenderConfiguration implements GelfAppenderConfiguration
    {
        @Override
        public String getRemoteHost()
        {
            return "localhost";
        }

        @Override
        public String getMessageOriginHost()
        {
            return "BrokerJ";
        }

        public GraylogAppender newAppender(Context context)
        {
            return GraylogAppender.newInstance(context, this);
        }
    }

    @Test
    public void testNewInstance()
    {
        TestGelfAppenderConfiguration logger = new TestGelfAppenderConfiguration();
        Context context = new LoggerContext();
        GraylogAppender appender = GraylogAppender.newInstance(context, logger);
        assertNotNull(appender);
    }

    @Test
    public void testStart()
    {
        TestGelfAppenderConfiguration logger = new TestGelfAppenderConfiguration();
        Context context = new LoggerContext();
        GraylogAppender appender = logger.newAppender(context);
        appender.setName("GelfAppender");
        appender.start();

        assertTrue(appender.isStarted());
        assertEquals("GelfAppender", appender.getName());

        Iterator<Appender<ILoggingEvent>> iterator = appender.iteratorForAppenders();
        Appender<ILoggingEvent> app = null;
        while (iterator.hasNext())
        {
            app = iterator.next();
            assertNotNull(app);
            assertTrue(app.isStarted());
            assertEquals("GelfAppender", app.getName());
            assertEquals(context, app.getContext());
            assertTrue(app instanceof GelfTcpAppender);
        }
        assertNotNull(app);
    }

    @Test
    public void testStart_again()
    {
        TestGelfAppenderConfiguration logger = new TestGelfAppenderConfiguration();
        Context context = new LoggerContext();
        GraylogAppender appender = logger.newAppender(context);
        appender.setName("GelfAppender");
        appender.start();
        assertTrue(appender.isStarted());

        Iterator<Appender<ILoggingEvent>> iterator = appender.iteratorForAppenders();
        List<Appender<ILoggingEvent>> appenders = new ArrayList<>();
        while (iterator.hasNext())
        {
            appenders.add(iterator.next());
        }
        assertFalse(appenders.isEmpty());

        appender.start();
        assertTrue(appender.isStarted());

        iterator = appender.iteratorForAppenders();
        List<Appender<ILoggingEvent>> newAppenders = new ArrayList<>();
        while (iterator.hasNext())
        {
            newAppenders.add(iterator.next());
        }
        assertFalse(newAppenders.isEmpty());
        assertTrue(newAppenders.containsAll(appenders));
        assertTrue(appenders.containsAll(newAppenders));
    }

    @Test
    public void testSetName()
    {
        TestGelfAppenderConfiguration configuration = new TestGelfAppenderConfiguration();
        Context context = new LoggerContext();
        GraylogAppender appender = configuration.newAppender(context);
        appender.setName("GelfAppender");
        appender.start();

        appender.setName("NewGelfAppender");
        Iterator<Appender<ILoggingEvent>> iterator = appender.iteratorForAppenders();
        while (iterator.hasNext())
        {
            Appender<ILoggingEvent> app = iterator.next();
            assertNotNull(app);
            assertEquals("NewGelfAppender", app.getName());
        }
    }

    @Test
    public void testSetContext()
    {
        TestGelfAppenderConfiguration logger = new TestGelfAppenderConfiguration();
        Context context = new LoggerContext();
        GraylogAppender appender = logger.newAppender(context);
        appender.setName("GelfAppender");
        appender.start();

        appender.setContext(context);
        Iterator<Appender<ILoggingEvent>> iterator = appender.iteratorForAppenders();
        while (iterator.hasNext())
        {
            Appender<ILoggingEvent> app = iterator.next();
            assertNotNull(app);
            assertEquals(context, app.getContext());
        }
    }

    @Test
    public void testRemoteHost()
    {
        TestGelfAppenderConfiguration configuration = new TestGelfAppenderConfiguration().withRemoteHost("Remote");
        Context context = new LoggerContext();
        GraylogAppender appender = configuration.newAppender(context);
        appender.start();

        GelfTcpAppender gelfAppender = extractGelfAppender(appender);
        assertNotNull(gelfAppender);
        assertEquals("Remote", gelfAppender.getGraylogHost());
    }

    @Test
    public void testRemoteHost_Default()
    {
        Context context = new LoggerContext();
        GraylogAppender appender = new DefaultGelfAppenderConfiguration().newAppender(context);
        appender.start();

        GelfTcpAppender gelfAppender = extractGelfAppender(appender);
        assertNotNull(gelfAppender);
        assertEquals("localhost", gelfAppender.getGraylogHost());
    }

    @Test
    public void testPort()
    {
        TestGelfAppenderConfiguration configuration = new TestGelfAppenderConfiguration().withPort(42456);
        Context context = new LoggerContext();
        GraylogAppender appender = configuration.newAppender(context);
        appender.start();

        GelfTcpAppender gelfAppender = extractGelfAppender(appender);
        assertNotNull(gelfAppender);
        assertEquals(42456, gelfAppender.getGraylogPort());
    }

    @Test
    public void testPort_Default()
    {
        Context context = new LoggerContext();
        GraylogAppender appender = new DefaultGelfAppenderConfiguration().newAppender(context);
        appender.start();

        GelfTcpAppender gelfAppender = extractGelfAppender(appender);
        assertNotNull(gelfAppender);
        assertEquals(12201, gelfAppender.getGraylogPort());
    }

    @Test
    public void testReconnectInterval_withRounding()
    {
        TestGelfAppenderConfiguration configuration = new TestGelfAppenderConfiguration().withReconnectionInterval(11456);
        Context context = new LoggerContext();
        GraylogAppender appender = configuration.newAppender(context);
        appender.start();

        GelfTcpAppender gelfAppender = extractGelfAppender(appender);
        assertNotNull(gelfAppender);
        assertEquals(12, gelfAppender.getReconnectInterval());
    }

    @Test
    public void testReconnectInterval_Default()
    {
        Context context = new LoggerContext();
        GraylogAppender appender = new DefaultGelfAppenderConfiguration().newAppender(context);
        appender.start();

        GelfTcpAppender gelfAppender = extractGelfAppender(appender);
        assertNotNull(gelfAppender);
        assertEquals(60, gelfAppender.getReconnectInterval());
    }

    @Test
    public void testReconnectInterval()
    {
        TestGelfAppenderConfiguration configuration = new TestGelfAppenderConfiguration().withReconnectionInterval(11000);
        Context context = new LoggerContext();
        GraylogAppender appender = configuration.newAppender(context);
        appender.start();

        GelfTcpAppender gelfAppender = extractGelfAppender(appender);
        assertNotNull(gelfAppender);
        assertEquals(11, gelfAppender.getReconnectInterval());
    }

    @Test
    public void testConnectTimeout()
    {
        TestGelfAppenderConfiguration configuration = new TestGelfAppenderConfiguration().withConnectionTimeout(1123);
        Context context = new LoggerContext();
        GraylogAppender appender = configuration.newAppender(context);
        appender.start();

        GelfTcpAppender gelfAppender = extractGelfAppender(appender);
        assertNotNull(gelfAppender);
        assertEquals(1123, gelfAppender.getConnectTimeout());
    }

    @Test
    public void testConnectTimeout_Default()
    {
        Context context = new LoggerContext();
        GraylogAppender appender = new DefaultGelfAppenderConfiguration().newAppender(context);
        appender.start();

        GelfTcpAppender gelfAppender = extractGelfAppender(appender);
        assertNotNull(gelfAppender);
        assertEquals(15000, gelfAppender.getConnectTimeout());
    }

    @Test
    public void testMaximumReconnectionAttempts()
    {
        TestGelfAppenderConfiguration configuration = new TestGelfAppenderConfiguration().withMaximumReconnectionAttempts(17);
        Context context = new LoggerContext();
        GraylogAppender appender = configuration.newAppender(context);
        appender.start();

        GelfTcpAppender gelfAppender = extractGelfAppender(appender);
        assertNotNull(gelfAppender);
        assertEquals(17, gelfAppender.getMaxRetries());
    }

    @Test
    public void testMaximumReconnectionAttempts_Default()
    {
        Context context = new LoggerContext();
        GraylogAppender appender = new DefaultGelfAppenderConfiguration().newAppender(context);
        appender.start();

        GelfTcpAppender gelfAppender = extractGelfAppender(appender);
        assertNotNull(gelfAppender);
        assertEquals(2, gelfAppender.getMaxRetries());
    }

    @Test
    public void testRetryDelay()
    {
        TestGelfAppenderConfiguration configuration = new TestGelfAppenderConfiguration().withRetryDelay(178);
        Context context = new LoggerContext();
        GraylogAppender appender = configuration.newAppender(context);
        appender.start();

        GelfTcpAppender gelfAppender = extractGelfAppender(appender);
        assertNotNull(gelfAppender);
        assertEquals(178, gelfAppender.getRetryDelay());
    }

    @Test
    public void testRetryDelay_Default()
    {
        Context context = new LoggerContext();
        GraylogAppender appender = new DefaultGelfAppenderConfiguration().newAppender(context);
        appender.start();

        GelfTcpAppender gelfAppender = extractGelfAppender(appender);
        assertNotNull(gelfAppender);
        assertEquals(3000, gelfAppender.getRetryDelay());
    }

    @Test
    public void testMessageOriginHost()
    {
        TestGelfAppenderConfiguration configuration = new TestGelfAppenderConfiguration().withMessageOriginHost("Broker");
        Context context = new LoggerContext();
        GraylogAppender appender = configuration.newAppender(context);
        appender.start();

        GelfEncoder gelfEncoder = extractGelfEncoder(appender);
        assertNotNull(gelfEncoder);
        assertEquals("Broker", gelfEncoder.getOriginHost());
    }

    @Test
    public void testRawMessageIncluded_True()
    {
        TestGelfAppenderConfiguration configuration = new TestGelfAppenderConfiguration().withRawMessageIncluded(true);
        Context context = new LoggerContext();
        GraylogAppender appender = configuration.newAppender(context);
        appender.start();

        GelfEncoder gelfEncoder = extractGelfEncoder(appender);
        assertNotNull(gelfEncoder);
        assertTrue(gelfEncoder.isIncludeRawMessage());
    }

    @Test
    public void testRawMessageIncluded_False()
    {
        TestGelfAppenderConfiguration configuration = new TestGelfAppenderConfiguration().withRawMessageIncluded(false);
        Context context = new LoggerContext();
        GraylogAppender appender = configuration.newAppender(context);
        appender.start();

        GelfEncoder gelfEncoder = extractGelfEncoder(appender);
        assertNotNull(gelfEncoder);
        assertFalse(gelfEncoder.isIncludeRawMessage());
    }

    @Test
    public void testRawMessageIncluded_Default()
    {
        Context context = new LoggerContext();
        GraylogAppender appender = new DefaultGelfAppenderConfiguration().newAppender(context);
        appender.start();

        GelfEncoder gelfEncoder = extractGelfEncoder(appender);
        assertNotNull(gelfEncoder);
        assertFalse(gelfEncoder.isIncludeRawMessage());
    }

    @Test
    public void testEventMarkerIncluded_True()
    {
        TestGelfAppenderConfiguration configuration = new TestGelfAppenderConfiguration().withEventMarkerIncluded(true);
        Context context = new LoggerContext();
        GraylogAppender appender = configuration.newAppender(context);
        appender.start();

        GelfEncoder gelfEncoder = extractGelfEncoder(appender);
        assertNotNull(gelfEncoder);
        assertTrue(gelfEncoder.isIncludeMarker());
    }

    @Test
    public void testEventMarkerIncluded_False()
    {
        TestGelfAppenderConfiguration configuration = new TestGelfAppenderConfiguration().withEventMarkerIncluded(false);
        Context context = new LoggerContext();
        GraylogAppender appender = configuration.newAppender(context);
        appender.start();

        GelfEncoder gelfEncoder = extractGelfEncoder(appender);
        assertNotNull(gelfEncoder);
        assertFalse(gelfEncoder.isIncludeMarker());
    }

    @Test
    public void testEventMarkerIncluded_Default()
    {
        Context context = new LoggerContext();
        GraylogAppender appender = new DefaultGelfAppenderConfiguration().newAppender(context);
        appender.start();

        GelfEncoder gelfEncoder = extractGelfEncoder(appender);
        assertNotNull(gelfEncoder);
        assertTrue(gelfEncoder.isIncludeMarker());
    }

    @Test
    public void testMdcPropertiesIncluded_True()
    {
        TestGelfAppenderConfiguration configuration = new TestGelfAppenderConfiguration().withMdcPropertiesIncluded(true);
        Context context = new LoggerContext();
        GraylogAppender appender = configuration.newAppender(context);
        appender.start();

        GelfEncoder gelfEncoder = extractGelfEncoder(appender);
        assertNotNull(gelfEncoder);
        assertTrue(gelfEncoder.isIncludeMdcData());
    }

    @Test
    public void testMdcPropertiesIncluded_False()
    {
        TestGelfAppenderConfiguration configuration = new TestGelfAppenderConfiguration().withMdcPropertiesIncluded(false);
        Context context = new LoggerContext();
        GraylogAppender appender = configuration.newAppender(context);
        appender.start();

        GelfEncoder gelfEncoder = extractGelfEncoder(appender);
        assertNotNull(gelfEncoder);
        assertFalse(gelfEncoder.isIncludeMdcData());
    }

    @Test
    public void testMdcPropertiesIncluded_Default()
    {
        Context context = new LoggerContext();
        GraylogAppender appender = new DefaultGelfAppenderConfiguration().newAppender(context);
        appender.start();

        GelfEncoder gelfEncoder = extractGelfEncoder(appender);
        assertNotNull(gelfEncoder);
        assertTrue(gelfEncoder.isIncludeMdcData());
    }

    @Test
    public void testCallerDataIncluded_True()
    {
        TestGelfAppenderConfiguration configuration = new TestGelfAppenderConfiguration().withCallerDataIncluded(true);
        Context context = new LoggerContext();
        GraylogAppender appender = configuration.newAppender(context);
        appender.start();

        GelfEncoder gelfEncoder = extractGelfEncoder(appender);
        assertNotNull(gelfEncoder);
        assertTrue(gelfEncoder.isIncludeCallerData());
    }

    @Test
    public void testCallerDataIncluded_False()
    {
        TestGelfAppenderConfiguration configuration = new TestGelfAppenderConfiguration().withCallerDataIncluded(false);
        Context context = new LoggerContext();
        GraylogAppender appender = configuration.newAppender(context);
        appender.start();

        GelfEncoder gelfEncoder = extractGelfEncoder(appender);
        assertNotNull(gelfEncoder);
        assertFalse(gelfEncoder.isIncludeCallerData());
    }

    @Test
    public void testCallerDataIncluded_Default()
    {
        Context context = new LoggerContext();
        GraylogAppender appender = new DefaultGelfAppenderConfiguration().newAppender(context);
        appender.start();

        GelfEncoder gelfEncoder = extractGelfEncoder(appender);
        assertNotNull(gelfEncoder);
        assertFalse(gelfEncoder.isIncludeCallerData());
    }

    @Test
    public void testRootExceptionDataIncluded_True()
    {
        TestGelfAppenderConfiguration configuration = new TestGelfAppenderConfiguration().withRootExceptionDataIncluded(true);
        Context context = new LoggerContext();
        GraylogAppender appender = configuration.newAppender(context);
        appender.start();

        GelfEncoder gelfEncoder = extractGelfEncoder(appender);
        assertNotNull(gelfEncoder);
        assertTrue(gelfEncoder.isIncludeRootCauseData());
    }

    @Test
    public void testRootExceptionDataIncluded_False()
    {
        TestGelfAppenderConfiguration configuration = new TestGelfAppenderConfiguration().withRootExceptionDataIncluded(false);
        Context context = new LoggerContext();
        GraylogAppender appender = configuration.newAppender(context);
        appender.start();

        GelfEncoder gelfEncoder = extractGelfEncoder(appender);
        assertNotNull(gelfEncoder);
        assertFalse(gelfEncoder.isIncludeRootCauseData());
    }

    @Test
    public void testRootExceptionDataIncluded_Default()
    {
        Context context = new LoggerContext();
        GraylogAppender appender = new DefaultGelfAppenderConfiguration().newAppender(context);
        appender.start();

        GelfEncoder gelfEncoder = extractGelfEncoder(appender);
        assertNotNull(gelfEncoder);
        assertFalse(gelfEncoder.isIncludeRootCauseData());
    }

    @Test
    public void testLogLevelNameIncluded_True()
    {
        TestGelfAppenderConfiguration configuration = new TestGelfAppenderConfiguration().withLogLevelNameIncluded(true);
        Context context = new LoggerContext();
        GraylogAppender appender = configuration.newAppender(context);
        appender.start();

        GelfEncoder gelfEncoder = extractGelfEncoder(appender);
        assertNotNull(gelfEncoder);
        assertTrue(gelfEncoder.isIncludeLevelName());
    }

    @Test
    public void testLogLevelNameIncluded_False()
    {
        TestGelfAppenderConfiguration configuration = new TestGelfAppenderConfiguration().withLogLevelNameIncluded(false);
        Context context = new LoggerContext();
        GraylogAppender appender = configuration.newAppender(context);
        appender.start();

        GelfEncoder gelfEncoder = extractGelfEncoder(appender);
        assertNotNull(gelfEncoder);
        assertFalse(gelfEncoder.isIncludeLevelName());
    }

    @Test
    public void testLogLevelNameIncluded_Default()
    {
        Context context = new LoggerContext();
        GraylogAppender appender = new DefaultGelfAppenderConfiguration().newAppender(context);
        appender.start();

        GelfEncoder gelfEncoder = extractGelfEncoder(appender);
        assertNotNull(gelfEncoder);
        assertFalse(gelfEncoder.isIncludeLevelName());
    }

    @Test
    public void testStaticFields()
    {
        Map<String, Object> staticFields = new HashMap<>(2);
        staticFields.put("A", "A.A");
        staticFields.put("B", 234);
        TestGelfAppenderConfiguration configuration = new TestGelfAppenderConfiguration().addStaticFields(staticFields);
        Context context = new LoggerContext();
        GraylogAppender appender = configuration.newAppender(context);
        appender.start();

        GelfEncoder gelfEncoder = extractGelfEncoder(appender);
        assertNotNull(gelfEncoder);
        Map<String, Object> fields = gelfEncoder.getStaticFields();
        assertNotNull(fields);
        assertEquals(2, fields.size());
        assertEquals("A.A", fields.get("A"));
        assertEquals(234, fields.get("B"));
    }

    @Test
    public void testStaticFields_Default()
    {
        Context context = new LoggerContext();
        GraylogAppender appender = new DefaultGelfAppenderConfiguration().newAppender(context);
        appender.start();

        GelfEncoder gelfEncoder = extractGelfEncoder(appender);
        assertNotNull(gelfEncoder);
        Map<String, Object> fields = gelfEncoder.getStaticFields();
        assertNotNull(fields);
        assertTrue(fields.isEmpty());
    }

    @Test
    public void testMessageBufferCapacity()
    {
        TestGelfAppenderConfiguration configuration = new TestGelfAppenderConfiguration().withMessageBufferCapacity(789);
        Context context = new LoggerContext();
        GraylogAppender appender = configuration.newAppender(context);
        appender.start();

        assertEquals(789, appender.getQueueSize());
    }

    @Test
    public void testMessageBufferCapacity_Default()
    {
        Context context = new LoggerContext();
        GraylogAppender appender = new DefaultGelfAppenderConfiguration().newAppender(context);
        appender.start();

        assertEquals(256, appender.getQueueSize());
    }

    @Test
    public void testMessagesFlushTimeOut()
    {
        TestGelfAppenderConfiguration configuration = new TestGelfAppenderConfiguration().withMessagesFlushTimeOut(567);
        Context context = new LoggerContext();
        GraylogAppender appender = configuration.newAppender(context);
        appender.start();

        assertEquals(567, appender.getMaxFlushTime());
    }

    @Test
    public void testMessagesFlushTimeOut_Default()
    {
        Context context = new LoggerContext();
        GraylogAppender appender = new DefaultGelfAppenderConfiguration().newAppender(context);
        appender.start();

        assertEquals(1000, appender.getMaxFlushTime());
    }

    @Test
    public void testDoAppend()
    {
        Context context = new LoggerContext();
        TestGelfAppenderConfiguration configuration = new TestGelfAppenderConfiguration();
        GraylogAppender appender = configuration.newAppender(context);
        try
        {
            appender.doAppend(new TestLoggingEvent());
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }

    private GelfTcpAppender extractGelfAppender(GraylogAppender appender)
    {
        Iterator<Appender<ILoggingEvent>> iterator = appender.iteratorForAppenders();
        while ((iterator.hasNext()))
        {
            Appender<ILoggingEvent> app = iterator.next();
            if (app instanceof GelfTcpAppender)
            {
                return (GelfTcpAppender) app;
            }
        }
        return null;
    }

    private GelfEncoder extractGelfEncoder(GraylogAppender appender)
    {
        Iterator<Appender<ILoggingEvent>> iterator = appender.iteratorForAppenders();
        while (iterator.hasNext())
        {
            Appender<ILoggingEvent> app = iterator.next();
            if (app instanceof GelfTcpAppender)
            {
                return ((GelfTcpAppender) app).getEncoder();
            }
        }
        return null;
    }
}