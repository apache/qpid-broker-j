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
package org.apache.qpid.server.exchange;

import java.security.AccessControlException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.security.auth.Subject;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.binding.BindingImpl;
import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.filter.AMQInvalidArgumentException;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.logging.messages.BindingMessages;
import org.apache.qpid.server.logging.messages.ExchangeMessages;
import org.apache.qpid.server.logging.subjects.ExchangeLogSubject;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.MessageSender;
import org.apache.qpid.server.message.RoutingResult;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.AlternateBinding;
import org.apache.qpid.server.model.Binding;
import org.apache.qpid.server.model.ConfiguredDerivedMethodAttribute;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.DoOnConfigThread;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.LifetimePolicy;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.Param;
import org.apache.qpid.server.model.PublishingLink;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.protocol.LinkModel;
import org.apache.qpid.server.queue.CreatingLinkInfo;
import org.apache.qpid.server.security.SecurityToken;
import org.apache.qpid.server.security.access.Operation;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.Deletable;
import org.apache.qpid.server.util.DeleteDeleteTask;
import org.apache.qpid.server.util.FixedKeyMapCreator;
import org.apache.qpid.server.virtualhost.MessageDestinationIsAlternateException;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.server.virtualhost.RequiredExchangeException;
import org.apache.qpid.server.virtualhost.ReservedExchangeNameException;
import org.apache.qpid.server.virtualhost.UnknownAlternateBindingException;
import org.apache.qpid.server.virtualhost.VirtualHostUnavailableException;

public abstract class AbstractExchange<T extends AbstractExchange<T>>
        extends AbstractConfiguredObject<T>
        implements Exchange<T>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractExchange.class);

    private static final ThreadLocal<Map<AbstractExchange<?>, Set<String>>> CURRENT_ROUTING = new ThreadLocal<>();

    private static final FixedKeyMapCreator BIND_ARGUMENTS_CREATOR =
            new FixedKeyMapCreator("bindingKey", "destination", "arguments");
    private static final FixedKeyMapCreator UNBIND_ARGUMENTS_CREATOR =
            new FixedKeyMapCreator("bindingKey", "destination");

    private static final Operation PUBLISH_ACTION = Operation.PERFORM_ACTION("publish");
    private final AtomicBoolean _closed = new AtomicBoolean();

    @ManagedAttributeField(beforeSet = "preSetAlternateBinding", afterSet = "postSetAlternateBinding" )
    private AlternateBinding _alternateBinding;
    @ManagedAttributeField
    private UnroutableMessageBehaviour _unroutableMessageBehaviour;
    @ManagedAttributeField
    private CreatingLinkInfo _creatingLinkInfo;

    private QueueManagingVirtualHost<?> _virtualHost;

    /**
     * Whether the exchange is automatically deleted once all queues have detached from it
     */
    private boolean _autoDelete;

    //The logSubject for ths exchange
    private LogSubject _logSubject;
    private final Set<DestinationReferrer> _referrers = Collections.newSetFromMap(new ConcurrentHashMap<DestinationReferrer,Boolean>());

    private final AtomicLong _receivedMessageCount = new AtomicLong();
    private final AtomicLong _receivedMessageSize = new AtomicLong();
    private final AtomicLong _routedMessageCount = new AtomicLong();
    private final AtomicLong _routedMessageSize = new AtomicLong();
    private final AtomicLong _droppedMessageCount = new AtomicLong();
    private final AtomicLong _droppedMessageSize = new AtomicLong();

    private final List<Binding> _bindings = new CopyOnWriteArrayList<>();

    private final ConcurrentMap<MessageSender, Integer> _linkedSenders = new ConcurrentHashMap<>();
    private final List<Action<? super Deletable<?>>> _deleteTaskList = new CopyOnWriteArrayList<>();
    private volatile MessageDestination _alternateBindingDestination;

    public AbstractExchange(Map<String, Object> attributes, QueueManagingVirtualHost<?> vhost)
    {
        super(vhost, attributes);
        Set<String> providedAttributeNames = new HashSet<>(attributes.keySet());
        providedAttributeNames.removeAll(getAttributeNames());
        if(!providedAttributeNames.isEmpty())
        {
            throw new IllegalArgumentException("Unknown attributes provided: " + providedAttributeNames);
        }
        _virtualHost = vhost;

        _logSubject = new ExchangeLogSubject(this, this.getVirtualHost());
    }

    @Override
    public void onValidate()
    {
        super.onValidate();

        if(!isSystemProcess())
        {
            if (isReservedExchangeName(getName()))
            {
                throw new ReservedExchangeNameException(getName());
            }
        }
    }

    @Override
    protected void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);

        validateOrCreateAlternateBinding(((Exchange<?>) proxyForValidation), false);

        if (changedAttributes.contains(ConfiguredObject.DESIRED_STATE) && proxyForValidation.getDesiredState() == State.DELETED)
        {
            doChecks();
        }

    }

    private boolean isReservedExchangeName(String name)
    {
        return name == null || ExchangeDefaults.DEFAULT_EXCHANGE_NAME.equals(name)
               || name.startsWith("amq.") || name.startsWith("qpid.");
    }

    @Override
    protected void validateOnCreate()
    {
        super.validateOnCreate();
        if (getCreatingLinkInfo() != null && !isSystemProcess())
        {
            throw new IllegalConfigurationException(String.format("Cannot specify creatingLinkInfo for exchange '%s'", getName()));
        }
    }

    @Override
    protected void onCreate()
    {
        super.onCreate();
        validateOrCreateAlternateBinding(this, true);
    }

    @Override
    protected void onOpen()
    {
        super.onOpen();
        final ConfiguredDerivedMethodAttribute<Exchange<?>, Collection<Binding>> durableBindingsAttribute =
                (ConfiguredDerivedMethodAttribute<Exchange<?>, Collection<Binding>>) getModel().getTypeRegistry().getAttributeTypes(getTypeClass()).get(DURABLE_BINDINGS);
        final Collection<Binding> bindings =
                durableBindingsAttribute.convertValue(getActualAttributes().get(DURABLE_BINDINGS), this);
        if (bindings != null)
        {
            _bindings.addAll(bindings);
            for (Binding b : _bindings)
            {
                final MessageDestination messageDestination = getOpenedMessageDestination(b.getDestination());
                if (messageDestination != null)
                {
                    Map<String, Object> arguments = b.getArguments() == null ? Collections.emptyMap() : b.getArguments();
                    try
                    {
                        onBind(new BindingIdentifier(b.getBindingKey(), messageDestination), arguments);
                    }
                    catch (AMQInvalidArgumentException e)
                    {
                        throw new IllegalConfigurationException("Unexpected bind argument : " + e.getMessage(), e);
                    }
                    messageDestination.linkAdded(this, b);
                }
            }
        }

        if (getLifetimePolicy() == LifetimePolicy.DELETE_ON_CREATING_LINK_CLOSE)
        {
            if (_creatingLinkInfo != null)
            {
                final LinkModel link;
                if (_creatingLinkInfo.isSendingLink())
                {
                    link = _virtualHost.getSendingLink(_creatingLinkInfo.getRemoteContainerId(), _creatingLinkInfo.getLinkName());
                }
                else
                {
                    link = _virtualHost.getReceivingLink(_creatingLinkInfo.getRemoteContainerId(), _creatingLinkInfo.getLinkName());
                }
                addLifetimeConstraint(link);
            }
            else
            {
                throw new IllegalArgumentException("Exchanges created with a lifetime policy of "
                                                   + getLifetimePolicy()
                                                   + " must be created from a AMQP 1.0 link.");
            }
        }

        if (getAlternateBinding() != null)
        {
            String alternateDestination = getAlternateBinding().getDestination();
            _alternateBindingDestination = getOpenedMessageDestination(alternateDestination);
            if (_alternateBindingDestination != null)
            {
                _alternateBindingDestination.addReference(this);
            }
            else
            {
                LOGGER.warn("Cannot find alternate binding destination '{}' for exchange '{}'", alternateDestination, toString());
            }
        }

        getEventLogger().message(ExchangeMessages.CREATED(getType(), getName(), isDurable()));
    }

    @Override
    public EventLogger getEventLogger()
    {
        return _virtualHost.getEventLogger();
    }

    private void performDeleteTasks()
    {
        for (Action<? super Deletable<?>> task : _deleteTaskList)
        {
            task.performAction(null);
        }

        _deleteTaskList.clear();
    }

    @Override
    public boolean isAutoDelete()
    {
        return getLifetimePolicy() != LifetimePolicy.PERMANENT;
    }

    private void addLifetimeConstraint(final Deletable<? extends Deletable> lifetimeObject)
    {
        final Action<Deletable> deleteExchangeTask = object -> Subject.doAs(getSubjectWithAddedSystemRights(),
                                                                            (PrivilegedAction<Void>) () ->
                                                                            {
                                                                                AbstractExchange.this.delete();
                                                                                return null;
                                                                            });

        lifetimeObject.addDeleteTask(deleteExchangeTask);
        _deleteTaskList.add(new DeleteDeleteTask(lifetimeObject, deleteExchangeTask));
    }

    private void performDelete()
    {
        if(_closed.compareAndSet(false,true))
        {
            performDeleteTasks();

            for(Binding b : _bindings)
            {
                final MessageDestination messageDestination = getAttainedMessageDestination(b.getDestination());
                if(messageDestination != null)
                {
                    messageDestination.linkRemoved(this, b);
                }
            }
            for(MessageSender sender : _linkedSenders.keySet())
            {
                sender.destinationRemoved(this);
            }

            if (_alternateBindingDestination != null)
            {
                _alternateBindingDestination.removeReference(AbstractExchange.this);
            }

            getEventLogger().message(_logSubject, ExchangeMessages.DELETED());
        }
    }

    private void doChecks()
    {
        if(hasReferrers())
        {
            throw new MessageDestinationIsAlternateException(getName());
        }

        if(isReservedExchangeName(getName()))
        {
            throw new RequiredExchangeException(getName());
        }
    }

    @Override
    @DoOnConfigThread
    public void destinationRemoved(@Param(name="destination") final MessageDestination destination)
    {
        Iterator<Binding> bindingIterator = _bindings.iterator();
        while(bindingIterator.hasNext())
        {
            Binding b = bindingIterator.next();
            if(b.getDestination().equals(destination.getName()))
            {
                final Map<String, Object> bindArguments =
                        UNBIND_ARGUMENTS_CREATOR.createMap(b.getBindingKey(), destination);
                getEventLogger().message(_logSubject, BindingMessages.DELETED(String.valueOf(bindArguments)));
                onUnbind(new BindingIdentifier(b.getBindingKey(), destination));
                _bindings.remove(b);
            }
        }
        if(!autoDeleteIfNecessary())
        {
            if (destination.isDurable() && isDurable())
            {
                final Collection<Binding> durableBindings = getDurableBindings();
                attributeSet(DURABLE_BINDINGS, durableBindings, durableBindings);
            }
        }
    }

    @Override
    public UnroutableMessageBehaviour getUnroutableMessageBehaviour()
    {
        return _unroutableMessageBehaviour;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + getName() +"]";
    }

    @Override
    public QueueManagingVirtualHost<?> getVirtualHost()
    {
        return _virtualHost;
    }

    @Override
    public boolean isBound(String bindingKey, Map<String,Object> arguments, Queue<?> queue)
    {
        if (bindingKey == null)
        {
            bindingKey = "";
        }
        for(Binding b : _bindings)
        {
            if(bindingKey.equals(b.getBindingKey()) && queue.getName().equals(b.getDestination()))
            {
                return (b.getArguments() == null || b.getArguments().isEmpty())
                       ? (arguments == null || arguments.isEmpty())
                       : b.getArguments().equals(arguments);
            }
        }
        return false;
    }

    @Override
    public boolean isBound(String bindingKey, Queue<?> queue)
    {
        if (bindingKey == null)
        {
            bindingKey = "";
        }

        for(Binding b : _bindings)
        {
            if(bindingKey.equals(b.getBindingKey()) && queue.getName().equals(b.getDestination()))
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isBound(String bindingKey)
    {
        if (bindingKey == null)
        {
            bindingKey = "";
        }

        for(Binding b : _bindings)
        {
            if(bindingKey.equals(b.getBindingKey()))
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isBound(Queue<?> queue)
    {
        for(Binding b : _bindings)
        {
            if(queue.getName().equals(b.getDestination()))
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isBound(Map<String, Object> arguments, Queue<?> queue)
    {
        for(Binding b : _bindings)
        {
            if(queue.getName().equals(b.getDestination()) &&
               ((b.getArguments() == null || b.getArguments().isEmpty())
                       ? (arguments == null || arguments.isEmpty())
                       : b.getArguments().equals(arguments)))
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isBound(Map<String, Object> arguments)
    {
        for(Binding b : _bindings)
        {
            if(((b.getArguments() == null || b.getArguments().isEmpty())
                                   ? (arguments == null || arguments.isEmpty())
                                   : b.getArguments().equals(arguments)))
            {
                return true;
            }
        }
        return false;
    }


    @Override
    public boolean isBound(String bindingKey, Map<String, Object> arguments)
    {
        if (bindingKey == null)
        {
            bindingKey = "";
        }

        for(Binding b : _bindings)
        {
            if(b.getBindingKey().equals(bindingKey) &&
               ((b.getArguments() == null || b.getArguments().isEmpty())
                       ? (arguments == null || arguments.isEmpty())
                       : b.getArguments().equals(arguments)))
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean hasBindings()
    {
        return !_bindings.isEmpty();
    }

    @Override
    public AlternateBinding getAlternateBinding()
    {
        return _alternateBinding;
    }

    private void preSetAlternateBinding()
    {
        if (_alternateBindingDestination != null)
        {
            _alternateBindingDestination.removeReference(this);
        }
    }

    @SuppressWarnings("unused")
    private void postSetAlternateBinding()
    {
        if(_alternateBinding != null)
        {
            _alternateBindingDestination = getOpenedMessageDestination(_alternateBinding.getDestination());
            if (_alternateBindingDestination != null)
            {
                _alternateBindingDestination.addReference(this);
            }
        }
    }

    @Override
    public MessageDestination getAlternateBindingDestination()
    {
        return _alternateBindingDestination;
    }

    @Override
    public void removeReference(DestinationReferrer destinationReferrer)
    {
        _referrers.remove(destinationReferrer);
    }

    @Override
    public void addReference(DestinationReferrer destinationReferrer)
    {
        _referrers.add(destinationReferrer);
    }

    private boolean hasReferrers()
    {
        return !_referrers.isEmpty();
    }

    @Override
    public Collection<Binding> getBindings()
    {
        return Collections.unmodifiableList(_bindings);
    }

    protected abstract void onBindingUpdated(final BindingIdentifier binding,
                                             final Map<String, Object> newArguments) throws AMQInvalidArgumentException;

    protected abstract void onBind(final BindingIdentifier binding, final Map<String, Object> arguments)
            throws AMQInvalidArgumentException;

    protected abstract void onUnbind(final BindingIdentifier binding);

    @Override
    public long getBindingCount()
    {
        return getBindings().size();
    }


    @Override
    public <M extends ServerMessage<? extends StorableMessageMetaData>> RoutingResult<M> route(final M message,
                                                                                               final String routingAddress,
                                                                                               final InstanceProperties instanceProperties)
    {
        if (_virtualHost.getState() != State.ACTIVE)
        {
            throw new VirtualHostUnavailableException(this._virtualHost);
        }

        final RoutingResult<M> routingResult = new RoutingResult<>(message);

        Map<AbstractExchange<?>, Set<String>> currentThreadMap = CURRENT_ROUTING.get();
        boolean topLevel = currentThreadMap == null;
        try
        {
            if (topLevel)
            {
                currentThreadMap = new HashMap<>();
                CURRENT_ROUTING.set(currentThreadMap);
            }
            Set<String> existingRoutes = currentThreadMap.get(this);
            if (existingRoutes == null)
            {
                currentThreadMap.put(this, Collections.singleton(routingAddress));
            }
            else if (existingRoutes.contains(routingAddress))
            {
                return routingResult;
            }
            else
            {
                existingRoutes = new HashSet<>(existingRoutes);
                existingRoutes.add(routingAddress);
                currentThreadMap.put(this, existingRoutes);
            }

            _receivedMessageCount.incrementAndGet();
            long sizeIncludingHeader = message.getSizeIncludingHeader();
            _receivedMessageSize.addAndGet(sizeIncludingHeader);

            doRoute(message, routingAddress, instanceProperties, routingResult);

            if (!routingResult.hasRoutes())
            {
                MessageDestination alternateBindingDestination = getAlternateBindingDestination();
                if (alternateBindingDestination != null)
                {
                    routingResult.add(alternateBindingDestination.route(message, routingAddress, instanceProperties));
                }
            }

            if (routingResult.hasRoutes())
            {
                _routedMessageCount.incrementAndGet();
                _routedMessageSize.addAndGet(sizeIncludingHeader);
            }
            else
            {
                _droppedMessageCount.incrementAndGet();
                _droppedMessageSize.addAndGet(sizeIncludingHeader);
            }

            return routingResult;
        }
        finally
        {
            if(topLevel)
            {
                CURRENT_ROUTING.set(null);
            }
        }
    }


    protected abstract <M extends ServerMessage<? extends StorableMessageMetaData>> void doRoute(final M message,
                                    final String routingAddress,
                                    final InstanceProperties instanceProperties,
                                    final RoutingResult<M> result);

    @Override
    public boolean bind(final String destination,
                        String bindingKey,
                        Map<String, Object> arguments,
                        boolean replaceExistingArguments)
    {
        try
        {
            return bindInternal(destination, bindingKey, arguments, replaceExistingArguments);
        }
        catch (AMQInvalidArgumentException e)
        {
            throw new IllegalArgumentException("Unexpected bind argument : " + e.getMessage(), e);
        }
    }

    private boolean bindInternal(final String destination,
                                 final String bindingKey,
                                 Map<String, Object> arguments,
                                 final boolean replaceExistingArguments) throws AMQInvalidArgumentException
    {
        MessageDestination messageDestination = getAttainedMessageDestination(destination);
        if (messageDestination == null)
        {
            throw new IllegalArgumentException(String.format("Destination '%s' is not found.", destination));
        }

        if(arguments == null)
        {
            arguments = Collections.emptyMap();
        }

        Binding newBinding = new BindingImpl(bindingKey, destination, arguments);

        Binding previousBinding = null;
        for(Binding b : _bindings)
        {
            if (b.getBindingKey().equals(bindingKey) && b.getDestination().equals(messageDestination.getName()))
            {
                previousBinding = b;
                break;
            }
        }

        if (previousBinding != null && !replaceExistingArguments)
        {
            return false;
        }


        final BindingIdentifier bindingIdentifier = new BindingIdentifier(bindingKey, messageDestination);
        if(previousBinding != null)
        {
            onBindingUpdated(bindingIdentifier, arguments);
        }
        else
        {
            final Map<String, Object> bindArguments =
                    BIND_ARGUMENTS_CREATOR.createMap(bindingKey, destination, arguments);
            getEventLogger().message(_logSubject, BindingMessages.CREATED(String.valueOf(bindArguments)));

            onBind(bindingIdentifier, arguments);
            messageDestination.linkAdded(this, newBinding);
        }

        if (previousBinding != null)
        {
            _bindings.remove(previousBinding);
        }
        _bindings.add(newBinding);
        if(isDurable() && messageDestination.isDurable())
        {
            final Collection<Binding> durableBindings = getDurableBindings();
            attributeSet(DURABLE_BINDINGS, durableBindings, durableBindings);
        }
        return true;
    }

    @Override
    public Collection<Binding> getPublishingLinks(MessageDestination destination)
    {
        List<Binding> bindings = new ArrayList<>();
        final String destinationName = destination.getName();
        for(Binding b : _bindings)
        {
            if(b.getDestination().equals(destinationName))
            {
                bindings.add(b);
            }
        }
        return bindings;
    }

    @Override
    public Collection<Binding> getDurableBindings()
    {
        List<Binding> durableBindings;
        if(isDurable())
        {
            durableBindings = new ArrayList<>();
            for (Binding b : _bindings)
            {
                MessageDestination destination = getAttainedMessageDestination(b.getDestination());
                if(destination != null && destination.isDurable())
                {
                    durableBindings.add(b);
                }
            }
        }
        else
        {
            durableBindings = Collections.emptyList();
        }
        return durableBindings;
    }

    @Override
    public CreatingLinkInfo getCreatingLinkInfo()
    {
        return _creatingLinkInfo;
    }

    private MessageDestination getAttainedMessageDestination(final String name)
    {
        MessageDestination destination = getVirtualHost().getAttainedQueue(name);
        return destination == null ? getVirtualHost().getAttainedMessageDestination(name, false) : destination;
    }

    private MessageDestination getOpenedMessageDestination(final String name)
    {
        MessageDestination destination = getVirtualHost().getSystemDestination(name);
        if(destination == null)
        {
            destination = getVirtualHost().getChildByName(Exchange.class, name);
        }

        if(destination == null)
        {
            destination = getVirtualHost().getChildByName(Queue.class, name);
        }
        return destination;
    }

    @Override
    public boolean unbind(@Param(name = "destination", mandatory = true) final String destination,
                          @Param(name = "bindingKey") String bindingKey)
    {
        MessageDestination messageDestination = getAttainedMessageDestination(destination);
        if (messageDestination != null)
        {
            Iterator<Binding> bindingIterator = _bindings.iterator();
            while (bindingIterator.hasNext())
            {
                Binding binding = bindingIterator.next();
                if (binding.getBindingKey().equals(bindingKey) && binding.getDestination().equals(destination))
                {
                    _bindings.remove(binding);
                    messageDestination.linkRemoved(this, binding);
                    onUnbind(new BindingIdentifier(bindingKey, messageDestination));
                    if (!autoDeleteIfNecessary())
                    {
                        if (isDurable() && messageDestination.isDurable())
                        {
                            final Collection<Binding> durableBindings = getDurableBindings();
                            attributeSet(DURABLE_BINDINGS, durableBindings, durableBindings);
                        }
                    }
                    final Map<String, Object> bindArguments =
                            UNBIND_ARGUMENTS_CREATOR.createMap(bindingKey, destination);
                    getEventLogger().message(_logSubject, BindingMessages.DELETED(String.valueOf(bindArguments)));

                    return true;
                }
            }
        }
        return false;

    }

    @Override
    public long getMessagesIn()
    {
        return _receivedMessageCount.get();
    }

    public long getMsgRoutes()
    {
        return _routedMessageCount.get();
    }

    @Override
    public long getMessagesDropped()
    {
        return _droppedMessageCount.get();
    }

    @Override
    public long getBytesIn()
    {
        return _receivedMessageSize.get();
    }

    public long getByteRoutes()
    {
        return _routedMessageSize.get();
    }

    @Override
    public long getBytesDropped()
    {
        return _droppedMessageSize.get();
    }

    @Override
    public boolean addBinding(String bindingKey, final Queue<?> queue, Map<String, Object> arguments)
            throws AMQInvalidArgumentException
    {
        return bindInternal(queue.getName(), bindingKey, arguments, false);
    }

    @Override
    public void replaceBinding(String bindingKey,
                               final Queue<?> queue,
                               Map<String, Object> arguments) throws AMQInvalidArgumentException
    {
        bindInternal(queue.getName(), bindingKey, arguments, true);
    }

    private boolean autoDeleteIfNecessary()
    {
        if (isAutoDeletePending())
        {
            LOGGER.debug("Auto-deleting exchange: {}", this);

            delete();
            return true;
        }
        else
        {
            return false;
        }
    }

    private boolean isAutoDeletePending()
    {
        return (getLifetimePolicy() == LifetimePolicy.DELETE_ON_NO_OUTBOUND_LINKS || getLifetimePolicy() == LifetimePolicy.DELETE_ON_NO_LINKS )
            && getBindingCount() == 0;
    }


    @SuppressWarnings("unused")
    @StateTransition(currentState = {State.UNINITIALIZED,State.ERRORED}, desiredState = State.ACTIVE)
    private ListenableFuture<Void> activate()
    {
        setState(State.ACTIVE);
        return Futures.immediateFuture(null);
    }

    @Override
    protected ListenableFuture<Void> onDelete()
    {
        if (getState() != State.UNINITIALIZED)
        {
            performDelete();
        }
        preSetAlternateBinding();
        return super.onDelete();
    }

    public static final class BindingIdentifier
    {
        private final String _bindingKey;
        private final MessageDestination _destination;

        public BindingIdentifier(final String bindingKey, final MessageDestination destination)
        {
            _bindingKey = bindingKey;
            _destination = destination;
        }

        public String getBindingKey()
        {
            return _bindingKey;
        }

        public MessageDestination getDestination()
        {
            return _destination;
        }

        @Override
        public boolean equals(final Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }

            final BindingIdentifier that = (BindingIdentifier) o;

            return _bindingKey.equals(that._bindingKey) && _destination.equals(that._destination);
        }

        @Override
        public int hashCode()
        {
            int result = _bindingKey.hashCode();
            result = 31 * result + _destination.hashCode();
            return result;
        }
    }

    // Used by the protocol layers
    @Override
    public boolean deleteBinding(final String bindingKey, final Queue<?> queue)
    {
        return unbind(queue.getName(), bindingKey);
    }

    @Override
    public boolean hasBinding(String bindingKey, final Queue<?> queue)
    {
        if (bindingKey == null)
        {
            bindingKey = "";
        }
        for (Binding b : _bindings)
        {
            if (b.getBindingKey().equals(bindingKey) && b.getDestination().equals(queue.getName()))
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public NamedAddressSpace getAddressSpace()
    {
        return _virtualHost;
    }

    @Override
    public void authorisePublish(final SecurityToken token, final Map<String, Object> arguments)
            throws AccessControlException
    {
        authorise(token, PUBLISH_ACTION, arguments);
    }

    @Override
    protected void logOperation(final String operation)
    {
        getEventLogger().message(ExchangeMessages.OPERATION(operation));
    }

    @Override
    public void linkAdded(final MessageSender sender, final PublishingLink link)
    {
        Integer oldValue = _linkedSenders.putIfAbsent(sender, 1);
        if(oldValue != null)
        {
            _linkedSenders.put(sender, oldValue+1);
        }
    }

    @Override
    public void linkRemoved(final MessageSender sender, final PublishingLink link)
    {
        int oldValue = _linkedSenders.remove(sender);
        if(oldValue != 1)
        {
            _linkedSenders.put(sender, oldValue-1);
        }
    }

    private void validateOrCreateAlternateBinding(final Exchange<?> exchange, final boolean mayCreate)
    {
        Object value = exchange.getAttribute(ALTERNATE_BINDING);
        if (value instanceof AlternateBinding)
        {
            AlternateBinding alternateBinding = (AlternateBinding) value;
            String destinationName = alternateBinding.getDestination();
            MessageDestination messageDestination =
                    _virtualHost.getAttainedMessageDestination(destinationName, mayCreate);
            if (messageDestination == null)
            {
                throw new UnknownAlternateBindingException(destinationName);
            }
            else if (messageDestination == this)
            {
                throw new IllegalConfigurationException(String.format(
                        "Cannot create alternate binding for '%s' : Alternate binding destination cannot refer to self.",
                        getName()));
            }
            else if (isDurable() && !messageDestination.isDurable())
            {
                throw new IllegalConfigurationException(String.format(
                        "Cannot create alternate binding for '%s' : Alternate binding destination '%s' is not durable.",
                        getName(),
                        destinationName));
            }
        }
    }
}
