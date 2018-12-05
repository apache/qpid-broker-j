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
package org.apache.qpid.server.virtualhostnode;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.messages.ConfigStoreMessages;
import org.apache.qpid.server.logging.subjects.MessageStoreLogSubject;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectTypeRegistry;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.IntegrityViolationException;
import org.apache.qpid.server.model.LifetimePolicy;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.plugin.ConfiguredObjectRegistration;
import org.apache.qpid.server.plugin.QpidServiceLoader;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.store.ConfiguredObjectRecord;
import org.apache.qpid.server.store.ConfiguredObjectRecordConverter;
import org.apache.qpid.server.store.ConfiguredObjectRecordImpl;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.preferences.NoopPreferenceStoreFactoryService;
import org.apache.qpid.server.store.preferences.PreferenceStore;
import org.apache.qpid.server.store.preferences.PreferenceStoreAttributes;
import org.apache.qpid.server.store.preferences.PreferenceStoreFactoryService;
import org.apache.qpid.server.util.urlstreamhandler.data.Handler;
import org.apache.qpid.server.virtualhost.NonStandardVirtualHost;
import org.apache.qpid.server.virtualhost.ProvidedStoreVirtualHostImpl;

public abstract class AbstractVirtualHostNode<X extends AbstractVirtualHostNode<X>> extends AbstractConfiguredObject<X> implements VirtualHostNode<X>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractVirtualHostNode.class);


    static
    {
        Handler.register();
    }

    private final Broker<?> _broker;
    private final EventLogger _eventLogger;

    private DurableConfigurationStore _durableConfigurationStore;

    private MessageStoreLogSubject _configurationStoreLogSubject;

    private volatile TaskExecutor _virtualHostExecutor;

    @ManagedAttributeField
    private boolean _defaultVirtualHostNode;

    @ManagedAttributeField
    private String _virtualHostInitialConfiguration;

    @ManagedAttributeField
    private PreferenceStoreAttributes _preferenceStoreAttributes;

    public AbstractVirtualHostNode(Broker<?> parent, Map<String, Object> attributes)
    {
        super(parent, attributes);
        _broker = parent;
        SystemConfig<?> systemConfig = getAncestor(SystemConfig.class);
        _eventLogger = systemConfig.getEventLogger();
    }

    @Override
    public void onOpen()
    {
        super.onOpen();
        _virtualHostExecutor = getTaskExecutor().getFactory().newInstance("VirtualHostNode-" + getName() + "-Config",
                                                                          () ->
                                                                          {
                                                                              VirtualHost<?> virtualHost = getVirtualHost();
                                                                              if (virtualHost != null)
                                                                              {
                                                                                  return virtualHost.getPrincipal();
                                                                              }
                                                                              return null;
                                                                          });
        _virtualHostExecutor.start();
        _durableConfigurationStore = createConfigurationStore();
    }

    @Override
    public TaskExecutor getChildExecutor()
    {
        return _virtualHostExecutor;
    }

    @Override
    public LifetimePolicy getLifetimePolicy()
    {
        return LifetimePolicy.PERMANENT;
    }

    @Override
    protected void onCreate()
    {
        super.onCreate();
    }

    @StateTransition(currentState = State.UNINITIALIZED, desiredState = State.QUIESCED)
    protected ListenableFuture<Void> startQuiesced()
    {
        setState(State.QUIESCED);
        return Futures.immediateFuture(null);
    }

    @StateTransition( currentState = {State.UNINITIALIZED, State.STOPPED, State.ERRORED }, desiredState = State.ACTIVE )
    protected ListenableFuture<Void> doActivate()
    {
        final SettableFuture<Void> returnVal = SettableFuture.create();

        try
        {
            addFutureCallback(activate(),
                                new FutureCallback<Void>()
                                {
                                    @Override
                                    public void onSuccess(final Void result)
                                    {
                                        try
                                        {
                                            setState(State.ACTIVE);
                                        }
                                        finally
                                        {
                                            returnVal.set(null);
                                        }

                                    }

                                    @Override
                                    public void onFailure(final Throwable t)
                                    {
                                        onActivationFailure(returnVal, t);
                                    }
                                }, getTaskExecutor()
                               );
        }
        catch(RuntimeException e)
        {
            onActivationFailure(returnVal, e);
        }
        return returnVal;
    }

    private void onActivationFailure(final SettableFuture<Void> returnVal, final Throwable e)
    {
        doAfterAlways(stopAndSetStateTo(State.ERRORED), () -> {
            if (_broker.isManagementMode())
            {
                LOGGER.warn("Failed to make " + this + " active.", e);
                returnVal.set(null);
            }
            else
            {
                returnVal.setException(e);
            }
        });
    }

    @Override
    public VirtualHost<?> getVirtualHost()
    {
        Collection<VirtualHost> children = new ArrayList<>(getChildren(VirtualHost.class));
        if (children.size() == 0)
        {
            return null;
        }
        else if (children.size() == 1)
        {
            return children.iterator().next();
        }
        else
        {
            throw new IllegalStateException(this + " has an unexpected number of virtualhost children, size " + children.size());
        }
    }

    @Override
    protected void validateOnCreate()
    {
        super.validateOnCreate();

        if (isDefaultVirtualHostNode())
        {
            VirtualHostNode existingDefault = _broker.findDefautVirtualHostNode();

            if (existingDefault != null)
            {
                throw new IllegalConfigurationException("The existing virtual host node '" + existingDefault.getName()
                                                      + "' is already the default for the Broker.");
            }
        }

    }

    @Override
    protected void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);
        VirtualHostNode updated = (VirtualHostNode) proxyForValidation;
        if (changedAttributes.contains(DEFAULT_VIRTUAL_HOST_NODE) && updated.isDefaultVirtualHostNode())
        {
            VirtualHostNode existingDefault = _broker.findDefautVirtualHostNode();

            if (existingDefault != null && existingDefault != this)
            {
                throw new IntegrityViolationException("Cannot make '" + getName() + "' the default virtual host node for"
                                                      + " the Broker as virtual host node '" + existingDefault.getName()
                                                      + "' is already the default.");
            }
        }
    }

    @Override
    public DurableConfigurationStore getConfigurationStore()
    {
        return _durableConfigurationStore;
    }

    protected Broker<?> getBroker()
    {
        return _broker;
    }

    protected EventLogger getEventLogger()
    {
        return _eventLogger;
    }

    protected MessageStoreLogSubject getConfigurationStoreLogSubject()
    {
        return _configurationStoreLogSubject;
    }

    @Override
    protected ListenableFuture<Void> onDelete()
    {
        throw new UnsupportedOperationException("Sub-classes must override");
    }

    protected ListenableFuture<Void> closeVirtualHostIfExists()
    {
        final VirtualHost<?> virtualHost = getVirtualHost();
        if (virtualHost != null)
        {
            return virtualHost.closeAsync();
        }
        else
        {
            return Futures.immediateFuture(null);
        }
    }

    @StateTransition( currentState = { State.ACTIVE, State.ERRORED, State.UNINITIALIZED }, desiredState = State.STOPPED )
    protected ListenableFuture<Void> doStop()
    {
        return stopAndSetStateTo(State.STOPPED);
    }

    protected ListenableFuture<Void> stopAndSetStateTo(final State stoppedState)
    {
        ListenableFuture<Void> childCloseFuture = closeChildren();
        return doAfterAlways(childCloseFuture, new Runnable()
        {
            @Override
            public void run()
            {
                closeConfigurationStoreSafely();
                setState(stoppedState);
            }
        });
    }

    @Override
    protected void onExceptionInOpen(RuntimeException e)
    {
        super.onExceptionInOpen(e);
        closeConfigurationStoreSafely();
    }

    @Override
    protected void postResolve()
    {
        super.postResolve();
        DurableConfigurationStore store = getConfigurationStore();
        if (store == null)
        {
            store = createConfigurationStore();
        }
        _configurationStoreLogSubject = new MessageStoreLogSubject(getName(), store.getClass().getSimpleName());
    }

    @Override
    protected ListenableFuture<Void> onClose()
    {
        closeConfigurationStore();
        onCloseOrDelete();
        return Futures.immediateFuture(null);
    }

    protected void onCloseOrDelete()
    {
        _virtualHostExecutor.stop();
    }

    private void closeConfigurationStore()
    {
        DurableConfigurationStore configurationStore = getConfigurationStore();
        if (configurationStore != null)
        {
            configurationStore.closeConfigurationStore();
            getEventLogger().message(getConfigurationStoreLogSubject(), ConfigStoreMessages.CLOSE());
        }
    }

    private void closeConfigurationStoreSafely()
    {
        try
        {
            closeConfigurationStore();
        }
        catch(Exception e)
        {
            LOGGER.warn("Unexpected exception on close of configuration store", e);
        }
    }

    @Override
    public String getVirtualHostInitialConfiguration()
    {
        return _virtualHostInitialConfiguration;
    }

    @Override
    public boolean isDefaultVirtualHostNode()
    {
        return _defaultVirtualHostNode;
    }

    @Override
    public PreferenceStoreAttributes getPreferenceStoreAttributes()
    {
        return _preferenceStoreAttributes;
    }

    @Override
    public PreferenceStore createPreferenceStore()
    {
        final Map<String, PreferenceStoreFactoryService> preferenceStoreFactories =
                new QpidServiceLoader().getInstancesByType(PreferenceStoreFactoryService.class);
        String preferenceStoreType;
        PreferenceStoreAttributes preferenceStoreAttributes = getPreferenceStoreAttributes();
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

    protected abstract DurableConfigurationStore createConfigurationStore();

    protected abstract ListenableFuture<Void> activate();

    protected abstract ConfiguredObjectRecord enrichInitialVirtualHostRootRecord(final ConfiguredObjectRecord vhostRecord);

    protected final ConfiguredObjectRecord[] getInitialRecords() throws IOException
    {
        ConfiguredObjectRecordConverter converter = new ConfiguredObjectRecordConverter(getModel());

        Collection<ConfiguredObjectRecord> records =
                new ArrayList<>(converter.readFromJson(VirtualHost.class,this,getInitialConfigReader()));

        if(!records.isEmpty())
        {
            ConfiguredObjectRecord vhostRecord = null;
            for(ConfiguredObjectRecord record : records)
            {
                if(record.getType().equals(VirtualHost.class.getSimpleName()))
                {
                    vhostRecord = record;
                    break;
                }
            }
            if(vhostRecord != null)
            {
                records.remove(vhostRecord);
                vhostRecord = enrichInitialVirtualHostRootRecord(vhostRecord);
                records.add(vhostRecord);
            }
            else
            {
                // this should be impossible as the converter should always generate a parent record
                throw new IllegalConfigurationException("Somehow the initial configuration has records but "
                                                        + "not a VirtualHost. This must be a coding error in Qpid");
            }
            addStandardExchangesIfNecessary(records, vhostRecord);
            enrichWithAuditInformation(records);
        }


        return records.toArray(new ConfiguredObjectRecord[records.size()]);
    }

    private void enrichWithAuditInformation(final Collection<ConfiguredObjectRecord> records)
    {
        List<ConfiguredObjectRecord> replacements = new ArrayList<>(records.size());

        for(ConfiguredObjectRecord record : records)
        {
            replacements.add(new ConfiguredObjectRecordImpl(record.getId(), record.getType(),
                                                            enrichAttributesWithAuditInformation(record.getAttributes()),
                                                            record.getParents()));
        }
        records.clear();
        records.addAll(replacements);
    }

    private Map<String, Object> enrichAttributesWithAuditInformation(final Map<String, Object> attributes)
    {
        LinkedHashMap<String,Object> enriched = new LinkedHashMap<>(attributes);
        final AuthenticatedPrincipal currentUser = AuthenticatedPrincipal.getCurrentUser();

        if(currentUser != null)
        {
            enriched.put(ConfiguredObject.LAST_UPDATED_BY, currentUser.getName());
            enriched.put(ConfiguredObject.CREATED_BY, currentUser.getName());
        }
        long currentTime = System.currentTimeMillis();
        enriched.put(ConfiguredObject.LAST_UPDATED_TIME, currentTime);
        enriched.put(ConfiguredObject.CREATED_TIME, currentTime);

        return enriched;
    }

    private void addStandardExchangesIfNecessary(final Collection<ConfiguredObjectRecord> records,
                                                 final ConfiguredObjectRecord vhostRecord)
    {
        addExchangeIfNecessary(ExchangeDefaults.FANOUT_EXCHANGE_CLASS, ExchangeDefaults.FANOUT_EXCHANGE_NAME, records, vhostRecord);
        addExchangeIfNecessary(ExchangeDefaults.HEADERS_EXCHANGE_CLASS, ExchangeDefaults.HEADERS_EXCHANGE_NAME, records, vhostRecord);
        addExchangeIfNecessary(ExchangeDefaults.TOPIC_EXCHANGE_CLASS, ExchangeDefaults.TOPIC_EXCHANGE_NAME, records, vhostRecord);
        addExchangeIfNecessary(ExchangeDefaults.DIRECT_EXCHANGE_CLASS, ExchangeDefaults.DIRECT_EXCHANGE_NAME, records, vhostRecord);
    }

    private void addExchangeIfNecessary(final String exchangeClass,
                                        final String exchangeName,
                                        final Collection<ConfiguredObjectRecord> records,
                                        final ConfiguredObjectRecord vhostRecord)
    {
        boolean found = false;

        for(ConfiguredObjectRecord record : records)
        {
            if(Exchange.class.getSimpleName().equals(record.getType())
               && exchangeName.equals(record.getAttributes().get(ConfiguredObject.NAME)))
            {
                found = true;
                break;
            }
        }

        if(!found)
        {
            final Map<String, Object> exchangeAttributes = new HashMap<>();
            exchangeAttributes.put(ConfiguredObject.NAME, exchangeName);
            exchangeAttributes.put(ConfiguredObject.TYPE, exchangeClass);

            records.add(new ConfiguredObjectRecordImpl(UUID.randomUUID(), Exchange.class.getSimpleName(),
                                                       exchangeAttributes, Collections.singletonMap(VirtualHost.class.getSimpleName(), vhostRecord.getId())));
        }
    }

    protected final Reader getInitialConfigReader() throws IOException
    {
        Reader initialConfigReader;
        if(getVirtualHostInitialConfiguration() != null)
        {
            String initialContextString = getVirtualHostInitialConfiguration();


            try
            {
                URL url = new URL(initialContextString);

                initialConfigReader =new InputStreamReader(url.openStream());
            }
            catch (MalformedURLException e)
            {
                initialConfigReader = new StringReader(initialContextString);
            }

        }
        else
        {
            LOGGER.warn("No initial configuration found for the virtual host");
            initialConfigReader = new StringReader("{}");
        }
        return initialConfigReader;
    }

    protected static Collection<String> getSupportedVirtualHostTypes(boolean includeProvided)
    {

        final Iterable<ConfiguredObjectRegistration> registrations =
                (new QpidServiceLoader()).instancesOf(ConfiguredObjectRegistration.class);

        Set<String> supportedTypes = new HashSet<>();

        for(ConfiguredObjectRegistration registration : registrations)
        {
            for(Class<? extends ConfiguredObject> typeClass : registration.getConfiguredObjectClasses())
            {
                if(VirtualHost.class.isAssignableFrom(typeClass))
                {
                    ManagedObject annotation = typeClass.getAnnotation(ManagedObject.class);

                    if (annotation.creatable() && annotation.defaultType().equals("") && !NonStandardVirtualHost.class.isAssignableFrom(typeClass))
                    {
                        supportedTypes.add(ConfiguredObjectTypeRegistry.getType(typeClass));
                    }
                }
            }
        }
        if(includeProvided)
        {
            supportedTypes.add(ProvidedStoreVirtualHostImpl.VIRTUAL_HOST_TYPE);
        }
        return Collections.unmodifiableCollection(supportedTypes);
    }

}
