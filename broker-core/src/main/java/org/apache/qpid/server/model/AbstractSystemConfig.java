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

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.security.auth.Subject;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.store.ManagementModeStoreHandler;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.CompositeStartupMessageLogger;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.MessageLogger;
import org.apache.qpid.server.logging.SystemOutMessageLogger;
import org.apache.qpid.server.logging.messages.BrokerMessages;
import org.apache.qpid.server.plugin.QpidServiceLoader;
import org.apache.qpid.server.store.ConfiguredObjectRecord;
import org.apache.qpid.server.store.ConfiguredObjectRecordConverter;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.handler.ConfiguredObjectRecordHandler;
import org.apache.qpid.server.store.preferences.NoopPreferenceStoreFactoryService;
import org.apache.qpid.server.store.preferences.PreferenceStore;
import org.apache.qpid.server.store.preferences.PreferenceStoreAttributes;
import org.apache.qpid.server.store.preferences.PreferenceStoreFactoryService;
import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.apache.qpid.server.util.urlstreamhandler.classpath.Handler;

public abstract class AbstractSystemConfig<X extends SystemConfig<X>>
        extends AbstractConfiguredObject<X> implements SystemConfig<X>, DynamicModel
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSystemConfig.class);

    private static final UUID SYSTEM_ID = new UUID(0l, 0l);
    private static final long SHUTDOWN_TIMEOUT = 30000l;

    private final Principal _systemPrincipal;

    private final EventLogger _eventLogger;

    private volatile DurableConfigurationStore _configurationStore;
    private Runnable _onContainerResolveTask;
    private Runnable _onContainerCloseTask;

    @ManagedAttributeField
    private boolean _managementMode;

    @ManagedAttributeField
    private int _managementModeHttpPortOverride;

    @ManagedAttributeField
    private boolean _managementModeQuiesceVirtualHosts;

    @ManagedAttributeField
    private String _managementModePassword;

    @ManagedAttributeField
    private String _initialConfigurationLocation;

    @ManagedAttributeField
    private String _initialSystemPropertiesLocation;

    @ManagedAttributeField
    private boolean _startupLoggedToSystemOut;

    @ManagedAttributeField
    private PreferenceStoreAttributes _preferenceStoreAttributes;

    @ManagedAttributeField
    private String _defaultContainerType;

    private final Thread _shutdownHook = new Thread(new ShutdownService(), "QpidBrokerShutdownHook");

    static
    {
        Handler.register();
    }

    public AbstractSystemConfig(final TaskExecutor taskExecutor,
                                final EventLogger eventLogger,
                                final Principal systemPrincipal,
                                final Map<String, Object> attributes)
    {
        super(null,
              updateAttributes(attributes),
              taskExecutor, SystemConfigBootstrapModel.getInstance());
        _eventLogger = eventLogger;
        _systemPrincipal = systemPrincipal;
        getTaskExecutor().start();
    }

    private static Map<String, Object> updateAttributes(Map<String, Object> attributes)
    {
        attributes = new HashMap<>(attributes);
        attributes.put(ConfiguredObject.NAME, "System");
        attributes.put(ID, SYSTEM_ID);
        return attributes;
    }

    @Override
    protected void setState(final State desiredState)
    {
        throw new IllegalArgumentException("Cannot change the state of the SystemContext object");
    }

    @Override
    public EventLogger getEventLogger()
    {
        return _eventLogger;
    }

    @Override
    protected ListenableFuture<Void> beforeClose()
    {
        try
        {
            boolean removed = Runtime.getRuntime().removeShutdownHook(_shutdownHook);
            LOGGER.debug("Removed shutdown hook : {}", removed);
        }
        catch(IllegalStateException ise)
        {
            //ignore, means the JVM is already shutting down
        }

        return super.beforeClose();
    }

    @Override
    protected ListenableFuture<Void> onClose()
    {
        final TaskExecutor taskExecutor = getTaskExecutor();
        try
        {

            if (taskExecutor != null)
            {
                taskExecutor.stop();
            }

            if (_configurationStore != null)
            {
                _configurationStore.closeConfigurationStore();
            }

        }
        finally
        {
            if (taskExecutor != null)
            {
                taskExecutor.stopImmediately();
            }
        }
        return Futures.immediateFuture(null);
    }

    @Override
    public final <T extends Container<? extends T>> T getContainer(Class<T> clazz)
    {
        Collection<? extends T> children = getChildren(clazz);
        if(children == null || children.isEmpty())
        {
            return null;
        }
        else if(children.size() != 1)
        {
            throw new IllegalConfigurationException("More than one " + clazz.getSimpleName() + " has been registered in a single context");
        }

        return children.iterator().next();

    }

    @Override
    public final Container<?> getContainer()
    {
        final Collection<Class<? extends ConfiguredObject>> containerTypes =
                getModel().getChildTypes(SystemConfig.class);
        Class containerClass = null;
        for(Class<? extends ConfiguredObject> clazz : containerTypes)
        {
            if(Container.class.isAssignableFrom(clazz))
            {
                if(containerClass == null)
                {
                    containerClass = clazz;
                }
                else
                {
                    throw new IllegalArgumentException("Model has more than one child Container class beneath SystemConfig");
                }
            }
        }

        if(containerClass == null)
        {
            throw new IllegalArgumentException("Model has no child Container class beneath SystemConfig");
        }

        return getContainer(containerClass);
    }

    @Override
    protected void onOpen()
    {
        super.onOpen();

        Runtime.getRuntime().addShutdownHook(_shutdownHook);
        LOGGER.debug("Added shutdown hook");

        _configurationStore = createStoreObject();

        if (isManagementMode())
        {
            _configurationStore = new ManagementModeStoreHandler(_configurationStore, this);
        }
    }


    @StateTransition(currentState = State.ACTIVE, desiredState = State.STOPPED)
    protected ListenableFuture<Void> doStop()
    {
        return doAfter(getContainer().closeAsync(), new Runnable()
        {
            @Override
            public void run()
            {
                _configurationStore.closeConfigurationStore();
                AbstractSystemConfig.super.setState(State.STOPPED);
            }
        });

    }


    @StateTransition(currentState = { State.UNINITIALIZED, State.STOPPED }, desiredState = State.ACTIVE)
    protected ListenableFuture<Void> activate()
    {
        return doAfter(makeActive(), new Runnable()
        {
            @Override
            public void run()
            {
                AbstractSystemConfig.super.setState(State.ACTIVE);
            }
        });
    }


    protected ListenableFuture<Void> makeActive()
    {

        final EventLogger eventLogger = _eventLogger;
        final EventLogger startupLogger = initiateStartupLogging();


        try
        {
            final Container<?> container = initiateStoreAndRecovery();

            container.setEventLogger(startupLogger);
            final SettableFuture<Void> returnVal = SettableFuture.create();
            addFutureCallback(container.openAsync(), new FutureCallback()
                                {
                                    @Override
                                    public void onSuccess(final Object result)
                                    {
                                        State state = container.getState();
                                        if (state == State.ACTIVE)
                                        {
                                            startupLogger.message(BrokerMessages.READY());
                                            container.setEventLogger(eventLogger);
                                            returnVal.set(null);
                                        }
                                        else
                                        {
                                            returnVal.setException(new ServerScopedRuntimeException("Broker failed reach ACTIVE state (state is " + state + ")"));
                                        }
                                    }

                                    @Override
                                    public void onFailure(final Throwable t)
                                    {
                                        returnVal.setException(t);
                                    }
                                }, getTaskExecutor()
                               );

            return returnVal;
        }
        catch (IOException e)
        {
            throw new IllegalArgumentException(e);
        }


    }


    private Container<?> initiateStoreAndRecovery() throws IOException
    {
        ConfiguredObjectRecord[] initialRecords = convertToConfigurationRecords(getInitialConfigurationLocation());
        final DurableConfigurationStore store = getConfigurationStore();
        store.init(AbstractSystemConfig.this);
        store.upgradeStoreStructure();
        final List<ConfiguredObjectRecord> records = new ArrayList<>();

        boolean isNew = store.openConfigurationStore(new ConfiguredObjectRecordHandler()
        {
            @Override
            public void handle(final ConfiguredObjectRecord record)
            {
                records.add(record);
            }
        }, initialRecords);

        String containerTypeName = getDefaultContainerType();
        for(ConfiguredObjectRecord record : records)
        {
            if(record.getParents() != null && record.getParents().size() == 1 && getId().equals(record.getParents().get(SystemConfig.class.getSimpleName())))
            {
                containerTypeName = record.getType();
                break;
            }
        }
        QpidServiceLoader loader = new QpidServiceLoader();
        final ContainerType<?> containerType = loader.getInstancesByType(ContainerType.class).get(containerTypeName);

        if(containerType != null)
        {
            if(containerType.getModel() != getModel())
            {
                updateModel(containerType.getModel());
            }
            containerType.getRecoverer(this).upgradeAndRecover(records);

        }
        else
        {
            throw new IllegalConfigurationException("Unknown container type '" + containerTypeName + "'");
        }

        final Class categoryClass = containerType.getCategoryClass();
        return (Container<?>) getContainer(categoryClass);
    }


    @StateTransition(currentState = State.UNINITIALIZED, desiredState = State.QUIESCED)
    protected ListenableFuture<Void> startQuiesced()
    {
        final EventLogger startupLogger = initiateStartupLogging();

        try
        {
            final Container<?> container = initiateStoreAndRecovery();

            container.setEventLogger(startupLogger);
            return Futures.immediateFuture(null);
        }
        catch (IOException e)
        {
            throw new IllegalArgumentException(e);
        }


    }

    private EventLogger initiateStartupLogging()
    {
        final EventLogger eventLogger = _eventLogger;

        final EventLogger startupLogger;
        if (isStartupLoggedToSystemOut())
        {
            //Create the composite (logging+SystemOut MessageLogger to be used during startup
            MessageLogger[] messageLoggers = {new SystemOutMessageLogger(), eventLogger.getMessageLogger()};

            CompositeStartupMessageLogger startupMessageLogger = new CompositeStartupMessageLogger(messageLoggers);
            startupLogger = new EventLogger(startupMessageLogger);
        }
        else
        {
            startupLogger = eventLogger;
        }
        return startupLogger;
    }


    @Override
    protected final boolean rethrowRuntimeExceptionsOnOpen()
    {
        return true;
    }

    protected abstract DurableConfigurationStore createStoreObject();

    @Override
    public DurableConfigurationStore getConfigurationStore()
    {
        return _configurationStore;
    }

    private ConfiguredObjectRecord[] convertToConfigurationRecords(final String initialConfigurationLocation) throws IOException
    {
        ConfiguredObjectRecordConverter converter = new ConfiguredObjectRecordConverter(getModel());

        Reader reader;

        try
        {
            URL url = new URL(initialConfigurationLocation);
            reader = new InputStreamReader(url.openStream());
        }
        catch (MalformedURLException e)
        {
            reader = new FileReader(initialConfigurationLocation);
        }

        try
        {
            Collection<ConfiguredObjectRecord> records =
                    converter.readFromJson(null, this, reader);
            return records.toArray(new ConfiguredObjectRecord[records.size()]);
        }
        finally
        {
            reader.close();
        }


    }

    @Override
    public String getDefaultContainerType()
    {
        return _defaultContainerType;
    }

    @Override
    public boolean isManagementMode()
    {
        return _managementMode;
    }

    @Override
    public int getManagementModeHttpPortOverride()
    {
        return _managementModeHttpPortOverride;
    }

    @Override
    public boolean isManagementModeQuiesceVirtualHosts()
    {
        return _managementModeQuiesceVirtualHosts;
    }

    @Override
    public String getManagementModePassword()
    {
        return _managementModePassword;
    }

    @Override
    public String getInitialConfigurationLocation()
    {
        return _initialConfigurationLocation;
    }

    @Override
    public String getInitialSystemPropertiesLocation()
    {
        return _initialSystemPropertiesLocation;
    }

    @Override
    public boolean isStartupLoggedToSystemOut()
    {
        return _startupLoggedToSystemOut;
    }


    @Override
    public PreferenceStoreAttributes getPreferenceStoreAttributes()
    {
        return _preferenceStoreAttributes;
    }

    @Override
    public PreferenceStore createPreferenceStore()
    {
        PreferenceStoreAttributes preferenceStoreAttributes = getPreferenceStoreAttributes();
        final Map<String, PreferenceStoreFactoryService> preferenceStoreFactories = new QpidServiceLoader().getInstancesByType(PreferenceStoreFactoryService.class);
        String preferenceStoreType;
        Map<String, Object> attributes;
        if (preferenceStoreAttributes == null)
        {
            preferenceStoreType = NoopPreferenceStoreFactoryService.TYPE;
            attributes = Collections.emptyMap();
        }
        else
        {
            preferenceStoreType = preferenceStoreAttributes.getType();
            attributes = preferenceStoreAttributes.getAttributes();
        }
        final PreferenceStoreFactoryService preferenceStoreFactory = preferenceStoreFactories.get(preferenceStoreType);
        return preferenceStoreFactory.createInstance(this, attributes);
    }

    @Override
    protected final Principal getSystemPrincipal()
    {
        return _systemPrincipal;
    }

    @Override
    public Runnable getOnContainerResolveTask()
    {
        return _onContainerResolveTask;
    }

    @Override
    public void setOnContainerResolveTask(final Runnable onContainerResolveTask)
    {
        _onContainerResolveTask = onContainerResolveTask;
    }

    @Override
    public Runnable getOnContainerCloseTask()
    {
        return _onContainerCloseTask;
    }

    @Override
    public void setOnContainerCloseTask(final Runnable onContainerCloseTask)
    {
        _onContainerCloseTask = onContainerCloseTask;
    }

    @Override
    protected void logOperation(final String operation)
    {
        getEventLogger().message(BrokerMessages.OPERATION(operation));
    }

    public static String getDefaultValue(String attrName)
    {
        Model model = SystemConfigBootstrapModel.getInstance();
        ConfiguredObjectTypeRegistry typeRegistry = model.getTypeRegistry();
        final ConfiguredObjectAttribute<?, ?> attr = typeRegistry.getAttributeTypes(SystemConfig.class).get(attrName);
        if(attr instanceof ConfiguredSettableAttribute)
        {
            return interpolate(model, ((ConfiguredSettableAttribute)attr).defaultValue());
        }
        else
        {
            return null;
        }
    }

    private class ShutdownService implements Runnable
    {
        @Override
        public void run()
        {
            Subject.doAs(getSystemTaskSubject("Shutdown"),
                         new PrivilegedAction<Object>()
                         {
                             @Override
                             public Object run()
                             {
                                 LOGGER.debug("Shutdown hook initiating close");
                                 ListenableFuture<Void> closeResult = closeAsync();
                                 try
                                 {
                                     closeResult.get(SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS);
                                 }
                                 catch (InterruptedException | ExecutionException  | TimeoutException e)
                                 {
                                     LOGGER.warn("Attempting to cleanly shutdown took too long, exiting immediately", e);
                                 }
                                 return null;
                             }
                         });
        }
    }

}
