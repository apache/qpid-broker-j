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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.security.auth.Subject;

import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.CommonProperties;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutorImpl;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.logging.LoggingMessageLogger;
import org.apache.qpid.server.logging.MessageLogger;
import org.apache.qpid.server.logging.SystemOutMessageLogger;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.plugin.PluggableFactoryLoader;
import org.apache.qpid.server.plugin.SystemConfigFactory;
import org.apache.qpid.server.security.auth.TaskPrincipal;
import org.apache.qpid.server.util.urlstreamhandler.classpath.Handler;

public class SystemLauncher
{

    private static final Logger LOGGER = LoggerFactory.getLogger(SystemLauncher.class);
    private static final String DEFAULT_INITIAL_PROPERTIES_LOCATION = "classpath:system.properties";

    private static final SystemLauncherListener.DefaultSystemLauncherListener DEFAULT_SYSTEM_LAUNCHER_LISTENER =
            new SystemLauncherListener.DefaultSystemLauncherListener();

    static
    {
        Handler.register();
    }


    private EventLogger _eventLogger;
    private final TaskExecutor _taskExecutor = new TaskExecutorImpl();

    private volatile SystemConfig _systemConfig;

    private SystemLauncherListener _listener;

    private final Principal _systemPrincipal = new SystemPrincipal();
    private final Subject _brokerTaskSubject;


    public SystemLauncher(SystemLauncherListener listener)
    {
        _listener = listener;
        _brokerTaskSubject = new Subject(true,
                                         new HashSet<>(Arrays.asList(_systemPrincipal, new TaskPrincipal("Broker"))),
                                         Collections.emptySet(),
                                         Collections.emptySet());

    }

    public SystemLauncher(SystemLauncherListener... listeners)
    {
        this(new SystemLauncherListener.ChainedSystemLauncherListener(listeners));
    }



    public SystemLauncher()
    {
        this(DEFAULT_SYSTEM_LAUNCHER_LISTENER);
    }

    public static void populateSystemPropertiesFromDefaults(final String initialProperties) throws IOException
    {
        URL initialPropertiesLocation;
        if(initialProperties == null)
        {
            initialPropertiesLocation = new URL(DEFAULT_INITIAL_PROPERTIES_LOCATION);
        }
        else
        {
            try
            {
                initialPropertiesLocation = new URL(initialProperties);
            }
            catch (MalformedURLException e)
            {
                initialPropertiesLocation = new File(initialProperties).toURI().toURL();

            }
        }

        Properties props = new Properties(CommonProperties.asProperties());

        try(InputStream inStream = initialPropertiesLocation.openStream())
        {
            props.load(inStream);
        }
        catch (FileNotFoundException e)
        {
            if(initialProperties != null)
            {
                throw e;
            }
        }

        Set<String> propertyNames = new HashSet<>(props.stringPropertyNames());
        propertyNames.removeAll(System.getProperties().stringPropertyNames());
        for (String propName : propertyNames)
        {
            System.setProperty(propName, props.getProperty(propName));
        }
    }

    public Principal getSystemPrincipal()
    {
        return _systemPrincipal;
    }

    public void shutdown()
    {
        shutdown(0);
    }

    public void shutdown(int exitStatusCode)
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
            _listener.exceptionOnShutdown(e);

        }
        catch(RuntimeException e)
        {
            _listener.exceptionOnShutdown(e);
            throw e;
        }
        finally
        {
            cleanUp(exitStatusCode);
        }
    }

    private void cleanUp(int exitStatusCode)
    {
        _taskExecutor.stop();

        _listener.onShutdown(exitStatusCode);

        _systemConfig = null;
    }


    public void startup(final Map<String,Object> systemConfigAttributes) throws Exception
    {
        final SystemOutMessageLogger systemOutMessageLogger = new SystemOutMessageLogger();

        _eventLogger = new EventLogger(systemOutMessageLogger);
        Subject.doAs(_brokerTaskSubject, new PrivilegedExceptionAction<Object>()
        {
            @Override
            public Object run() throws Exception
            {
                _listener.beforeStartup();

                try
                {
                    startupImpl(systemConfigAttributes);
                }
                catch (RuntimeException e)
                {
                    systemOutMessageLogger.message(new SystemStartupMessage(e));
                    LOGGER.error("Exception during startup", e);
                    _listener.errorOnStartup(e);
                    closeSystemConfigAndCleanUp();
                }
                finally
                {
                    _listener.afterStartup();
                }
                return null;
            }
        });

    }

    private void startupImpl(Map<String,Object> systemConfigAttributes) throws Exception
    {
        populateSystemPropertiesFromDefaults((String) systemConfigAttributes.get(SystemConfig.INITIAL_SYSTEM_PROPERTIES_LOCATION));

        String storeType = (String) systemConfigAttributes.get(SystemConfig.TYPE);

        // Create the RootLogger to be used during broker operation
        boolean statusUpdatesEnabled = Boolean.parseBoolean(System.getProperty(SystemConfig.PROPERTY_STATUS_UPDATES, "true"));
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
        _systemConfig = configFactory.newInstance(_taskExecutor,
                                                  _eventLogger,
                                                  _systemPrincipal,
                                                  systemConfigAttributes);

        _systemConfig.setOnContainerResolveTask(
                new Runnable()
                {
                    @Override
                    public void run()
                    {
                        _listener.onContainerResolve(_systemConfig);
                    }
                });

        _systemConfig.setOnContainerCloseTask(
                new Runnable()
                {
                    @Override
                    public void run()
                    {
                        _listener.onContainerClose(_systemConfig);

                    }
                });


        _systemConfig.open();
        if (_systemConfig.getContainer().getState() == State.ERRORED)
        {
            throw new RuntimeException("Closing due to errors");
        }
    }

    private void closeSystemConfigAndCleanUp()
    {
        try
        {
            if (_systemConfig != null)
            {
                try
                {
                    _systemConfig.close();
                }
                catch (Exception ce)
                {
                    LOGGER.debug("An error occurred when closing the system config following initialization failure", ce);
                }
            }
        }
        finally
        {
            cleanUp(1);
        }
    }

    private static final class SystemPrincipal implements Principal, Serializable
    {
        private static final long serialVersionUID = 1L;

        private SystemPrincipal()
        {
        }

        @Override
        public String getName()
        {
            return "SYSTEM";
        }
    }

    private static class SystemStartupMessage implements LogMessage
    {
        private final RuntimeException _exception;

        public SystemStartupMessage(final RuntimeException exception)
        {
            _exception = exception;
        }

        @Override
        public String getLogHierarchy()
        {
            return "system";
        }

        @Override
        public String toString()
        {
            StringWriter writer = new StringWriter();
            _exception.printStackTrace(new PrintWriter(writer));
            return "Exception during startup: \n" + writer.toString();
        }
    }
}
