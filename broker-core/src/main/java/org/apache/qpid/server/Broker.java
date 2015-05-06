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
package org.apache.qpid.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.security.auth.Subject;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;
import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.common.QpidProperties;
import org.apache.qpid.server.configuration.BrokerProperties;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutorImpl;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.LoggingMessageLogger;
import org.apache.qpid.server.logging.MessageLogger;
import org.apache.qpid.server.logging.SystemOutMessageLogger;
import org.apache.qpid.server.logging.log4j.LoggingManagementFacade;
import org.apache.qpid.server.logging.messages.BrokerMessages;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.plugin.PluggableFactoryLoader;
import org.apache.qpid.server.plugin.SystemConfigFactory;
import org.apache.qpid.server.security.SecurityManager;
import org.apache.qpid.server.util.Action;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Broker
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Broker.class);

    private volatile Thread _shutdownHookThread;
    private EventLogger _eventLogger;
    private boolean _configuringOwnLogging = false;
    private final TaskExecutor _taskExecutor = new TaskExecutorImpl();

    private SystemConfig _systemConfig;

    private final Action<Integer> _shutdownAction;


    public Broker()
    {
        this(null);
    }

    public Broker(Action<Integer> shutdownAction)
    {
        _shutdownAction = shutdownAction;
    }

    protected static class InitException extends RuntimeException
    {
        private static final long serialVersionUID = 1L;

        InitException(String msg, Throwable cause)
        {
            super(msg, cause);
        }
    }

    public void shutdown()
    {
        shutdown(0);
    }

    public void shutdown(int exitStatusCode)
    {
        try
        {
            removeShutdownHook();
        }
        finally
        {
            try
            {
                if(_systemConfig != null)
                {
                    ListenableFuture<Void> closeResult = _systemConfig.closeAsync();
                    closeResult.get(30000l, TimeUnit.MILLISECONDS);
                }

            }
            catch (TimeoutException | InterruptedException | ExecutionException e)
            {
                LOGGER.warn("Attempting to cleanly shutdown took too long, exiting immediately");
            }
            finally
            {
                cleanUp(exitStatusCode);
            }
        }
    }

    private void cleanUp(int exitStatusCode)
    {
        _taskExecutor.stop();

        if (_configuringOwnLogging)
        {
            LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
            loggerContext.stop();
        }

        if (_shutdownAction != null)
        {
            _shutdownAction.performAction(exitStatusCode);
        }

        _systemConfig = null;
    }

    public void startup() throws Exception
    {
        startup(new BrokerOptions());
    }

    public void startup(final BrokerOptions options) throws Exception
    {
        _eventLogger = new EventLogger(new SystemOutMessageLogger());

        Subject.doAs(SecurityManager.getSystemTaskSubject("Broker"), new PrivilegedExceptionAction<Object>()
        {
            @Override
            public Object run() throws Exception
            {
                startupImpl(options);
                return null;
            }
        });

    }

    private void startupImpl(final BrokerOptions options) throws Exception
    {
        populateSystemPropertiesFromDefaults(options.getInitialSystemProperties());

        String storeLocation = options.getConfigurationStoreLocation();
        String storeType = options.getConfigurationStoreType();

        // Create the RootLogger to be used during broker operation
        boolean statusUpdatesEnabled = Boolean.parseBoolean(System.getProperty(BrokerProperties.PROPERTY_STATUS_UPDATES, "true"));

        ch.qos.logback.classic.Logger logger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        logger.getLoggerContext().reset();
        logger.setAdditive(true);
        logger.setLevel(Level.INFO);

        MessageLogger messageLogger = new LoggingMessageLogger(statusUpdatesEnabled);
        _eventLogger.setMessageLogger(messageLogger);


        PluggableFactoryLoader<SystemConfigFactory> configFactoryLoader = new PluggableFactoryLoader<>(SystemConfigFactory.class);
        SystemConfigFactory configFactory = configFactoryLoader.get(storeType);
        if(configFactory == null)
        {
            LOGGER.error("Unknown config store type '" + storeType + "', only the following types are supported: " + configFactoryLoader.getSupportedTypes());
            throw new IllegalArgumentException("Unknown config store type '"+storeType+"', only the following types are supported: " + configFactoryLoader.getSupportedTypes());
        }


        _taskExecutor.start();
        _systemConfig = configFactory.newInstance(_taskExecutor, _eventLogger, options.convertToSystemConfigAttributes());
        try
        {
            _systemConfig.open();
            if (_systemConfig.getBroker().getState() == State.ERRORED)
            {
                LOGGER.warn("Closing broker as it cannot operate due to errors");
                closeSystemConfig();
            }
            else
            {
                addShutdownHook();
            }
        }
        catch(RuntimeException e)
        {
            LOGGER.error("Exception during startup", e);
            closeSystemConfig();
            throw e;
        }

    }

    private void closeSystemConfig()
    {
        try
        {
            _systemConfig.close();
        }
        catch(Exception ce)
        {
            LOGGER.debug("An error occurred when closing the system config following initialization failure", ce);
        }
        finally
        {
            cleanUp(1);
        }
    }


    private void addShutdownHook()
    {
        Thread shutdownHookThread = new Thread(new ShutdownService());
        shutdownHookThread.setName("QpidBrokerShutdownHook");

        Runtime.getRuntime().addShutdownHook(shutdownHookThread);
        _shutdownHookThread = shutdownHookThread;

        LOGGER.debug("Added shutdown hook");
    }

    private void removeShutdownHook()
    {
        Thread shutdownThread = _shutdownHookThread;

        //if there is a shutdown thread and we aren't it, we should remove it
        if(shutdownThread != null && !(Thread.currentThread() == shutdownThread))
        {
            LOGGER.debug("Removing shutdown hook");

            _shutdownHookThread = null;

            boolean removed = false;
            try
            {
                removed = Runtime.getRuntime().removeShutdownHook(shutdownThread);
            }
            catch(IllegalStateException ise)
            {
                //ignore, means the JVM is already shutting down
            }

            if(LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Removed shutdown hook: " + removed);
            }
        }
        else
        {
            LOGGER.debug("Skipping shutdown hook removal as there either isn't one, or we are it.");
        }
    }

    public static void populateSystemPropertiesFromDefaults(final String initialProperties) throws IOException
    {
        URL initialPropertiesLocation;
        if(initialProperties == null)
        {
            initialPropertiesLocation = Broker.class.getClassLoader().getResource("system.properties");
        }
        else
        {
            initialPropertiesLocation = (new File(initialProperties)).toURI().toURL();
        }

        Properties props = new Properties(QpidProperties.asProperties());
        if(initialPropertiesLocation != null)
        {

            try(InputStream inStream = initialPropertiesLocation.openStream())
            {
                props.load(inStream);
            }
        }

        Set<String> propertyNames = new HashSet<>(props.stringPropertyNames());
        propertyNames.removeAll(System.getProperties().stringPropertyNames());
        for (String propName : propertyNames)
        {
            System.setProperty(propName, props.getProperty(propName));
        }
    }


    private class ShutdownService implements Runnable
    {
        public void run()
        {
            Subject.doAs(SecurityManager.getSystemTaskSubject("Shutdown"), new PrivilegedAction<Object>()
            {
                @Override
                public Object run()
                {
                    LOGGER.debug("Shutdown hook running");
                    Broker.this.shutdown();
                    return null;
                }
            });
        }
    }

}
