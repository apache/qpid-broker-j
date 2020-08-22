package org.apache.qpid.server.logging.logback.validator;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.logging.logback.graylog.GelfAppenderConfiguration;
import org.apache.qpid.test.utils.UnitTestBase;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class GelfConfigurationValidatorTest extends UnitTestBase
{
    private static class TestLogger implements GelfAppenderConfiguration
    {
        private int _port = 12201;

        private int _reconnectionInterval = 10000;

        private int _connectionTimeout = 300;

        private int _maximumReconnectionAttempts = 1;

        private int _retryDelay = 500;

        private int _messagesFlushTimeOut = 10000;

        private int _messageBufferCapacity = 10000;

        private final Map<String, Object> _staticFields = new LinkedHashMap<>();

        @Override
        public String getRemoteHost()
        {
            return "localhost";
        }

        @Override
        public int getPort()
        {
            return _port;
        }

        public TestLogger withPort(int port)
        {
            this._port = port;
            return this;
        }

        @Override
        public int getReconnectionInterval()
        {
            return _reconnectionInterval;
        }

        public TestLogger withReconnectionInterval(int reconnectionInterval)
        {
            this._reconnectionInterval = reconnectionInterval;
            return this;
        }

        @Override
        public int getConnectionTimeout()
        {
            return _connectionTimeout;
        }

        public TestLogger withConnectionTimeout(int connectionTimeout)
        {
            this._connectionTimeout = connectionTimeout;
            return this;
        }

        @Override
        public int getMaximumReconnectionAttempts()
        {
            return _maximumReconnectionAttempts;
        }

        public TestLogger withMaximumReconnectionAttempts(int maximumReconnectionAttempts)
        {
            this._maximumReconnectionAttempts = maximumReconnectionAttempts;
            return this;
        }

        @Override
        public int getRetryDelay()
        {
            return _retryDelay;
        }

        public TestLogger withRetryDelay(int retryDelay)
        {
            this._retryDelay = retryDelay;
            return this;
        }

        @Override
        public int getMessagesFlushTimeOut()
        {
            return _messagesFlushTimeOut;
        }

        public TestLogger withMessagesFlushTimeOut(int messagesFlushTimeOut)
        {
            this._messagesFlushTimeOut = messagesFlushTimeOut;
            return this;
        }

        @Override
        public int getMessageBufferCapacity()
        {
            return _messageBufferCapacity;
        }

        public TestLogger withMessageBufferCapacity(int messageBufferCapacity)
        {
            this._messageBufferCapacity = messageBufferCapacity;
            return this;
        }

        @Override
        public String getMessageOriginHost()
        {
            return "BrokerJ";
        }

        @Override
        public boolean isRawMessageIncluded()
        {
            return true;
        }

        @Override
        public boolean isEventMarkerIncluded()
        {
            return true;
        }

        @Override
        public boolean hasMdcPropertiesIncluded()
        {
            return true;
        }

        @Override
        public boolean isCallerDataIncluded()
        {
            return true;
        }

        @Override
        public boolean hasRootExceptionDataIncluded()
        {
            return true;
        }

        @Override
        public boolean isLogLevelNameIncluded()
        {
            return true;
        }

        @Override
        public Map<String, Object> getStaticFields()
        {
            return _staticFields;
        }

        public TestLogger addStaticFields(Map<String, ?> map)
        {
            this._staticFields.putAll(map);
            return this;
        }
    }

    @Test
    public void testValidate_Port_ValidInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        TestLogger logger = new TestLogger();
        try
        {
            GelfConfigurationValidator.validateConfiguration(logger.withPort(4567), object);
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        try
        {
            GelfConfigurationValidator.validateConfiguration(logger.withPort(4567), object, Collections.singleton(GelfConfigurationValidator.PORT.attributeName()));
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        try
        {
            GelfConfigurationValidator.PORT.validate(logger.withPort(4567), object);
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }

    @Test
    public void testValidate_Port_InvalidInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        TestLogger logger = new TestLogger();
        try
        {
            GelfConfigurationValidator.validateConfiguration(logger.withPort(456789), object);
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'port' instance of org.apache.qpid.server.logging.logback.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '456789' as it has to be in range [1, 65535]", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            GelfConfigurationValidator.PORT.validate(logger.withPort(456789), object);
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'port' instance of org.apache.qpid.server.logging.logback.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '456789' as it has to be in range [1, 65535]", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            GelfConfigurationValidator.validateConfiguration(logger.withPort(456789), object, Collections.singleton("A"));
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }

    @Test
    public void testValidate_ReconnectionInterval_ValidInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        TestLogger logger = new TestLogger();
        try
        {
            GelfConfigurationValidator.validateConfiguration(logger.withReconnectionInterval(500), object);
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        try
        {
            GelfConfigurationValidator.validateConfiguration(logger.withReconnectionInterval(500), object, Collections.singleton(GelfConfigurationValidator.RECONNECTION_INTERVAL.attributeName()));
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        try
        {
            GelfConfigurationValidator.RECONNECTION_INTERVAL.validate(logger.withReconnectionInterval(500), object);
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }

    @Test
    public void testValidate_ReconnectionInterval_InvalidInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        TestLogger logger = new TestLogger();
        try
        {
            GelfConfigurationValidator.validateConfiguration(logger.withReconnectionInterval(-1), object);
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'reconnectionInterval' instance of org.apache.qpid.server.logging.logback.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '-1' as it has to be at least 0", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            GelfConfigurationValidator.RECONNECTION_INTERVAL.validate(logger.withReconnectionInterval(-1), object);
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'reconnectionInterval' instance of org.apache.qpid.server.logging.logback.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '-1' as it has to be at least 0", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            GelfConfigurationValidator.validateConfiguration(logger.withReconnectionInterval(-1), object, Collections.singleton("A"));
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }

    @Test
    public void testValidate_ConnectionTimeout_ValidInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        TestLogger logger = new TestLogger();
        try
        {
            GelfConfigurationValidator.validateConfiguration(logger.withConnectionTimeout(500), object);
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        try
        {
            GelfConfigurationValidator.validateConfiguration(logger.withConnectionTimeout(500), object, Collections.singleton(GelfConfigurationValidator.CONNECTION_TIMEOUT.attributeName()));
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        try
        {
            GelfConfigurationValidator.CONNECTION_TIMEOUT.validate(logger.withConnectionTimeout(500), object);
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }

    @Test
    public void testValidate_ConnectionTimeout_InvalidInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        TestLogger logger = new TestLogger();
        try
        {
            GelfConfigurationValidator.validateConfiguration(logger.withConnectionTimeout(-1), object);
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'connectionTimeout' instance of org.apache.qpid.server.logging.logback.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '-1' as it has to be at least 0", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            GelfConfigurationValidator.CONNECTION_TIMEOUT.validate(logger.withConnectionTimeout(-1), object);
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'connectionTimeout' instance of org.apache.qpid.server.logging.logback.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '-1' as it has to be at least 0", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            GelfConfigurationValidator.validateConfiguration(logger.withConnectionTimeout(-1), object, Collections.singleton("A"));
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }

    @Test
    public void testValidate_MaximumReconnectionAttempts_ValidInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        TestLogger logger = new TestLogger();
        try
        {
            GelfConfigurationValidator.validateConfiguration(logger.withMaximumReconnectionAttempts(5), object);
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        try
        {
            GelfConfigurationValidator.validateConfiguration(logger.withMaximumReconnectionAttempts(5), object, Collections.singleton(GelfConfigurationValidator.MAXIMUM_RECONNECTION_ATTEMPTS.attributeName()));
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        try
        {
            GelfConfigurationValidator.MAXIMUM_RECONNECTION_ATTEMPTS.validate(logger.withMaximumReconnectionAttempts(5), object);
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

    }

    @Test
    public void testValidate_MaximumReconnectionAttempts_InvalidInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        TestLogger logger = new TestLogger();
        try
        {
            GelfConfigurationValidator.validateConfiguration(logger.withMaximumReconnectionAttempts(-5), object);
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'maximumReconnectionAttempts' instance of org.apache.qpid.server.logging.logback.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '-5' as it has to be at least 0", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            GelfConfigurationValidator.MAXIMUM_RECONNECTION_ATTEMPTS.validate(logger.withMaximumReconnectionAttempts(-5), object);
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'maximumReconnectionAttempts' instance of org.apache.qpid.server.logging.logback.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '-5' as it has to be at least 0", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            GelfConfigurationValidator.validateConfiguration(logger.withMaximumReconnectionAttempts(-1), object, Collections.singleton("A"));
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }

    @Test
    public void testValidate_RetryDelay_ValidInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        TestLogger logger = new TestLogger();
        try
        {
            GelfConfigurationValidator.validateConfiguration(logger.withRetryDelay(50), object);
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        try
        {
            GelfConfigurationValidator.validateConfiguration(logger.withRetryDelay(50), object, Collections.singleton(GelfConfigurationValidator.RETRY_DELAY.attributeName()));
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        try
        {
            GelfConfigurationValidator.RETRY_DELAY.validate(logger.withRetryDelay(50), object);
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }

    @Test
    public void testValidate_RetryDelay_InvalidInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        TestLogger logger = new TestLogger();
        try
        {
            GelfConfigurationValidator.validateConfiguration(logger.withRetryDelay(-5), object);
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'retryDelay' instance of org.apache.qpid.server.logging.logback.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '-5' as it has to be at least 0", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            GelfConfigurationValidator.RETRY_DELAY.validate(logger.withRetryDelay(-5), object);
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'retryDelay' instance of org.apache.qpid.server.logging.logback.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '-5' as it has to be at least 0", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            GelfConfigurationValidator.validateConfiguration(logger.withRetryDelay(-5), object, Collections.singleton("A"));
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }

    @Test
    public void testValidate_MessagesFlushTimeOut_ValidInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        TestLogger logger = new TestLogger();
        try
        {
            GelfConfigurationValidator.validateConfiguration(logger.withMessagesFlushTimeOut(50), object);
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        try
        {
            GelfConfigurationValidator.validateConfiguration(logger.withMessagesFlushTimeOut(50), object, Collections.singleton(GelfConfigurationValidator.FLUSH_TIME_OUT.attributeName()));
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        try
        {
            GelfConfigurationValidator.FLUSH_TIME_OUT.validate(logger.withMessagesFlushTimeOut(50), object);
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }

    @Test
    public void testValidate_MessagesFlushTimeOut_InvalidInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        TestLogger logger = new TestLogger();
        try
        {
            GelfConfigurationValidator.validateConfiguration(logger.withMessagesFlushTimeOut(-5), object);
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'messagesFlushTimeOut' instance of org.apache.qpid.server.logging.logback.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '-5' as it has to be at least 0", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            GelfConfigurationValidator.FLUSH_TIME_OUT.validate(logger.withMessagesFlushTimeOut(-5), object);
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'messagesFlushTimeOut' instance of org.apache.qpid.server.logging.logback.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '-5' as it has to be at least 0", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            GelfConfigurationValidator.validateConfiguration(logger.withMessagesFlushTimeOut(-5), object, Collections.singleton("A"));
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }

    @Test
    public void testValidate_MessageBufferCapacity_ValidInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        TestLogger logger = new TestLogger();
        try
        {
            GelfConfigurationValidator.validateConfiguration(logger.withMessageBufferCapacity(250), object);
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        try
        {
            GelfConfigurationValidator.validateConfiguration(logger.withMessageBufferCapacity(250), object, Collections.singleton(GelfConfigurationValidator.BUFFER_CAPACITY.attributeName()));
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        try
        {
            GelfConfigurationValidator.BUFFER_CAPACITY.validate(logger.withMessageBufferCapacity(250), object);
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }

    @Test
    public void testValidate_MessageBufferCapacity_InvalidInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        TestLogger logger = new TestLogger();
        try
        {
            GelfConfigurationValidator.validateConfiguration(logger.withMessageBufferCapacity(0), object);
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'messageBufferCapacity' instance of org.apache.qpid.server.logging.logback.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '0' as it has to be at least 1", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            GelfConfigurationValidator.BUFFER_CAPACITY.validate(logger.withMessageBufferCapacity(0), object);
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'messageBufferCapacity' instance of org.apache.qpid.server.logging.logback.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '0' as it has to be at least 1", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            GelfConfigurationValidator.validateConfiguration(logger.withMessageBufferCapacity(0), object, Collections.singleton("A"));
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }

    @Test
    public void testValidate_StaticFields_ValidInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        TestLogger logger = new TestLogger();
        try
        {
            GelfConfigurationValidator.validateConfiguration(logger.addStaticFields(Collections.singletonMap("A", 345)), object);
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        try
        {
            GelfConfigurationValidator.validateConfiguration(logger.addStaticFields(Collections.singletonMap("A", 345)), object, Collections.singleton(GelfConfigurationValidator.STATIC_FIELDS.attributeName()));
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        try
        {
            GelfConfigurationValidator.STATIC_FIELDS.validate(logger.addStaticFields(Collections.singletonMap("A", 345)), object);
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }

    @Test
    public void testValidate_StaticFields_InvalidInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        TestLogger logger = new TestLogger();
        try
        {
            GelfConfigurationValidator.validateConfiguration(logger.addStaticFields(Collections.singletonMap("A", true)), object);
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Value of 'staticFields attribute instance of org.apache.qpid.server.logging.logback.validator.TestConfiguredObject named 'TestConfiguredObject' cannot be 'true', as it has to be a string or number", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            GelfConfigurationValidator.STATIC_FIELDS.validate(logger.addStaticFields(Collections.singletonMap("A", true)), object);
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Value of 'staticFields attribute instance of org.apache.qpid.server.logging.logback.validator.TestConfiguredObject named 'TestConfiguredObject' cannot be 'true', as it has to be a string or number", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            GelfConfigurationValidator.validateConfiguration(logger.addStaticFields(Collections.singletonMap("A", true)), object, Collections.singleton("A"));
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }
}