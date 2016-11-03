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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.exchange.ExchangeDefaults;
import org.apache.qpid.server.binding.BindingImpl;
import org.apache.qpid.server.configuration.updater.Task;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.logging.messages.ExchangeMessages;
import org.apache.qpid.server.logging.subjects.ExchangeLogSubject;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.Binding;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.LifetimePolicy;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.Publisher;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.queue.BaseQueue;
import org.apache.qpid.server.security.SecurityToken;
import org.apache.qpid.server.security.access.Operation;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.virtualhost.ExchangeIsAlternateException;
import org.apache.qpid.server.virtualhost.RequiredExchangeException;
import org.apache.qpid.server.virtualhost.ReservedExchangeNameException;
import org.apache.qpid.server.virtualhost.VirtualHostUnavailableException;

public abstract class AbstractExchange<T extends AbstractExchange<T>>
        extends AbstractConfiguredObject<T>
        implements Exchange<T>
{
    private static final Logger _logger = LoggerFactory.getLogger(AbstractExchange.class);
    private static final Operation PUBLISH_ACTION = Operation.ACTION("publish");
    private final AtomicBoolean _closed = new AtomicBoolean();

    @ManagedAttributeField(beforeSet = "preSetAlternateExchange", afterSet = "postSetAlternateExchange" )
    private Exchange<?> _alternateExchange;
    @ManagedAttributeField
    private UnroutableMessageBehaviour _unroutableMessageBehaviour;

    private VirtualHost<?> _virtualHost;

    /**
     * Whether the exchange is automatically deleted once all queues have detached from it
     */
    private boolean _autoDelete;

    //The logSubject for ths exchange
    private LogSubject _logSubject;
    private Map<ExchangeReferrer,Object> _referrers = new ConcurrentHashMap<ExchangeReferrer,Object>();

    private final CopyOnWriteArrayList<Binding<?>> _bindings = new CopyOnWriteArrayList<>();
    private final AtomicLong _receivedMessageCount = new AtomicLong();
    private final AtomicLong _receivedMessageSize = new AtomicLong();
    private final AtomicLong _routedMessageCount = new AtomicLong();
    private final AtomicLong _routedMessageSize = new AtomicLong();
    private final AtomicLong _droppedMessageCount = new AtomicLong();
    private final AtomicLong _droppedMessageSize = new AtomicLong();

    private final ConcurrentMap<BindingIdentifier, Binding<?>> _bindingsMap = new ConcurrentHashMap<>();

    public AbstractExchange(Map<String, Object> attributes, VirtualHost<?> vhost)
    {
        super(parentsMap(vhost), attributes);
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

    private boolean isReservedExchangeName(String name)
    {
        return name == null || ExchangeDefaults.DEFAULT_EXCHANGE_NAME.equals(name)
               || name.startsWith("amq.") || name.startsWith("qpid.");
    }


    @Override
    protected void onOpen()
    {
        super.onOpen();

        // Log Exchange creation
        getEventLogger().message(ExchangeMessages.CREATED(getType(), getName(), isDurable()));
    }

    @Override
    public EventLogger getEventLogger()
    {
        return _virtualHost.getEventLogger();
    }

    public boolean isAutoDelete()
    {
        return getLifetimePolicy() != LifetimePolicy.PERMANENT;
    }

    private ListenableFuture<Void> deleteWithChecks()
    {
        if(hasReferrers())
        {
            throw new ExchangeIsAlternateException(getName());
        }

        if(isReservedExchangeName(getName()))
        {
            throw new RequiredExchangeException(getName());
        }

        if(_closed.compareAndSet(false,true))
        {
            List<ListenableFuture<Void>> removeBindingFutures = new ArrayList<>(_bindings.size());

            List<Binding<?>> bindings = new ArrayList<>(_bindings);
            for(Binding<?> binding : bindings)
            {
                removeBindingFutures.add(binding.deleteAsync());
            }

            ListenableFuture<List<Void>> combinedFuture = Futures.allAsList(removeBindingFutures);
            return doAfter(combinedFuture, new Runnable()
            {
                @Override
                public void run()
                {
                    if (_alternateExchange != null)
                    {
                        _alternateExchange.removeReference(AbstractExchange.this);
                    }

                    getEventLogger().message(_logSubject, ExchangeMessages.DELETED());

                    deleted();
                }
            });
        }
        else
        {
            deleted();
            return Futures.immediateFuture(null);
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

    public VirtualHost<?> getVirtualHost()
    {
        return _virtualHost;
    }

    public final boolean isBound(String bindingKey, Map<String,Object> arguments, Queue<?> queue)
    {
        for(Binding<?> b : _bindings)
        {
            if(bindingKey.equals(b.getBindingKey()) && queue == b.getQueue())
            {
                return (b.getArguments() == null || b.getArguments().isEmpty())
                       ? (arguments == null || arguments.isEmpty())
                       : b.getArguments().equals(arguments);
            }
        }
        return false;
    }

    public final boolean isBound(String bindingKey, Queue<?> queue)
    {
        for(Binding<?> b : _bindings)
        {
            if(bindingKey.equals(b.getBindingKey()) && queue == b.getQueue())
            {
                return true;
            }
        }
        return false;
    }

    public final boolean isBound(String bindingKey)
    {
        for(Binding<?> b : _bindings)
        {
            if(bindingKey.equals(b.getBindingKey()))
            {
                return true;
            }
        }
        return false;
    }

    public final boolean isBound(Queue<?> queue)
    {
        for(Binding<?> b : _bindings)
        {
            if(queue == b.getQueue())
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public final boolean isBound(Map<String, Object> arguments, Queue<?> queue)
    {
        for(Binding<?> b : _bindings)
        {
            if(queue == b.getQueue() &&
               ((b.getArguments() == null || b.getArguments().isEmpty())
                       ? (arguments == null || arguments.isEmpty())
                       : b.getArguments().equals(arguments)))
            {
                return true;
            }
        }
        return false;
    }


    public final boolean isBound(Map<String, Object> arguments)
    {
        for(Binding<?> b : _bindings)
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
    public final boolean isBound(String bindingKey, Map<String, Object> arguments)
    {
        for(Binding<?> b : _bindings)
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

    public final boolean hasBindings()
    {
        return !_bindings.isEmpty();
    }

    public Exchange<?> getAlternateExchange()
    {
        return _alternateExchange;
    }

    private void preSetAlternateExchange()
    {
        if (_alternateExchange != null)
        {
            _alternateExchange.removeReference(this);
        }
    }

    @SuppressWarnings("unused")
    private void postSetAlternateExchange()
    {
        if(_alternateExchange != null)
        {
            _alternateExchange.addReference(this);
        }
    }

    public void removeReference(ExchangeReferrer exchange)
    {
        _referrers.remove(exchange);
    }

    public void addReference(ExchangeReferrer exchange)
    {
        _referrers.put(exchange, Boolean.TRUE);
    }

    public boolean hasReferrers()
    {
        return !_referrers.isEmpty();
    }

    public final void doAddBinding(final BindingImpl binding)
    {
        _bindings.add(binding);
        onBind(binding);
    }

    public final void doRemoveBinding(final Binding<?> binding)
    {
        onUnbind(binding);
        _bindings.remove(binding);
    }

    public final Collection<Binding<?>> getBindings()
    {
        return Collections.unmodifiableList(_bindings);
    }

    protected abstract void onBind(final Binding<?> binding);

    protected abstract void onUnbind(final Binding<?> binding);

    public Map<String, Object> getArguments()
    {
        return Collections.emptyMap();
    }

    public long getBindingCount()
    {
        return getBindings().size();
    }


    final List<? extends BaseQueue> route(final ServerMessage message,
                                          final String routingAddress,
                                          final InstanceProperties instanceProperties)
    {
        _receivedMessageCount.incrementAndGet();
        _receivedMessageSize.addAndGet(message.getSize());
        List<? extends BaseQueue> queues = doRoute(message, routingAddress, instanceProperties);
        List<? extends BaseQueue> allQueues = queues;

        boolean deletedQueues = false;

        for(BaseQueue q : allQueues)
        {
            if(q.isDeleted())
            {
                if(!deletedQueues)
                {
                    deletedQueues = true;
                    queues = new ArrayList<>(allQueues);
                }
                _logger.debug("Exchange: {} - attempt to enqueue message onto deleted queue {}", getName(), q.getName());

                queues.remove(q);
            }
        }


        if(!queues.isEmpty())
        {
            _routedMessageCount.incrementAndGet();
            _routedMessageSize.addAndGet(message.getSize());
        }
        else
        {
            _droppedMessageCount.incrementAndGet();
            _droppedMessageSize.addAndGet(message.getSize());
        }
        return queues;
    }

    public final  <M extends ServerMessage<? extends StorableMessageMetaData>> int send(final M message,
                                                                                        final String routingAddress,
                                                                                        final InstanceProperties instanceProperties,
                                                                                        final ServerTransaction txn,
                                                                                        final Action<? super MessageInstance> postEnqueueAction)
    {
        if (_virtualHost.getState() != State.ACTIVE)
        {
            throw new VirtualHostUnavailableException(this._virtualHost);
        }

        List<? extends BaseQueue> queues = route(message, routingAddress, instanceProperties);
        if(queues == null || queues.isEmpty())
        {
            Exchange altExchange = getAlternateExchange();
            if(altExchange != null)
            {
                return altExchange.send(message, routingAddress, instanceProperties, txn, postEnqueueAction);
            }
            else
            {
                return 0;
            }
        }
        else
        {
            final BaseQueue[] baseQueues;

            if(message.isReferenced())
            {
                ArrayList<BaseQueue> uniqueQueues = new ArrayList<>(queues.size());
                for(BaseQueue q : queues)
                {
                    if(!message.isReferenced(q))
                    {
                        uniqueQueues.add(q);
                    }
                }
                baseQueues = uniqueQueues.toArray(new BaseQueue[uniqueQueues.size()]);
            }
            else
            {
                baseQueues = queues.toArray(new BaseQueue[queues.size()]);
            }

            txn.enqueue(queues,message, new ServerTransaction.EnqueueAction()
            {
                MessageReference _reference = message.newReference();

                public void postCommit(MessageEnqueueRecord... records)
                {
                    try
                    {
                        for(int i = 0; i < baseQueues.length; i++)
                        {
                            baseQueues[i].enqueue(message, postEnqueueAction, records[i]);
                        }
                    }
                    finally
                    {
                        _reference.release();
                    }
                }

                public void onRollback()
                {
                    _reference.release();
                }
            });
            return queues.size();
        }
    }

    protected abstract List<? extends BaseQueue> doRoute(final ServerMessage message,
                                                         final String routingAddress,
                                                         final InstanceProperties instanceProperties);

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
    public boolean addBinding(final String bindingKey, final Queue<?> queue, final Map<String, Object> arguments)
    {
        return doSync(doOnConfigThread(new Task<ListenableFuture<Boolean>, RuntimeException>()
        {
            @Override
            public ListenableFuture<Boolean> execute()
            {
                return makeBindingAsync(null, bindingKey, queue, arguments, false);
            }

            @Override
            public String getObject()
            {
                return AbstractExchange.this.toString();
            }

            @Override
            public String getAction()
            {
                return "add binding";
            }

            @Override
            public String getArguments()
            {
                return "bindingKey=" + bindingKey + ", queue=" + queue + ", arguments=" + arguments;
            }
        }));


    }

    @Override
    public boolean replaceBinding(final String bindingKey,
                                  final Queue<?> queue,
                                  final Map<String, Object> arguments)
    {
        return doSync(doOnConfigThread(new Task<ListenableFuture<Boolean>, RuntimeException>()
        {
            @Override
            public ListenableFuture<Boolean> execute()
            {

                final Binding<?> existingBinding = getBinding(bindingKey, queue);
                return makeBindingAsync(existingBinding == null ? null : existingBinding.getId(),
                                   bindingKey,
                                   queue,
                                   arguments,
                                   true);
            }

            @Override
            public String getObject()
            {
                return AbstractExchange.this.toString();
            }

            @Override
            public String getAction()
            {
                return "replace binding";
            }

            @Override
            public String getArguments()
            {
                return "bindingKey=" + bindingKey + ", queue=" + queue + ", arguments=" + arguments;
            }
        }));
    }

    @Override
    public ListenableFuture<Void> removeBindingAsync(final Binding<?> binding)
    {
        String bindingKey = binding.getBindingKey();
        Queue<?> queue = binding.getQueue();

        assert queue != null;

        if (bindingKey == null)
        {
            bindingKey = "";
        }

        // Check access
        binding.authorise(Operation.DELETE);

        Binding<?> b = _bindingsMap.remove(new BindingIdentifier(bindingKey,queue));

        if (b != null)
        {
            doRemoveBinding(b);
            queue.removeBinding(b);

            // TODO - RG - Fix bindings!
            return autoDeleteIfNecessaryAsync();
        }
        else
        {
            return Futures.immediateFuture(null);
        }

    }

    private ListenableFuture<Void> autoDeleteIfNecessaryAsync()
    {
        if (isAutoDeletePending())
        {
            _logger.debug("Auto-deleting exchange: {}", this);

            return deleteAsync();
        }

        return Futures.immediateFuture(null);
    }

    private void autoDeleteIfNecessary()
    {
        if (isAutoDeletePending())
        {
            _logger.debug("Auto-deleting exchange: {}", this);

            delete();
        }
    }

    private boolean isAutoDeletePending()
    {
        return (getLifetimePolicy() == LifetimePolicy.DELETE_ON_NO_OUTBOUND_LINKS || getLifetimePolicy() == LifetimePolicy.DELETE_ON_NO_LINKS )
            && getBindingCount() == 0;
    }

    public Binding<?> getBinding(String bindingKey, Queue<?> queue)
    {
        assert queue != null;

        if(bindingKey == null)
        {
            bindingKey = "";
        }

        return _bindingsMap.get(new BindingIdentifier(bindingKey,queue));
    }

    private ListenableFuture<Boolean> makeBindingAsync(UUID id,
                                String bindingKey,
                                Queue<?> queue,
                                Map<String, Object> arguments,
                                boolean force)
    {
        if (bindingKey == null)
        {
            bindingKey = "";
        }
        if (arguments == null)
        {
            arguments = Collections.emptyMap();
        }

        if (id == null)
        {
            id = UUID.randomUUID();
        }

        Binding<?> existingMapping;
        synchronized(this)
        {
            BindingIdentifier bindingIdentifier = new BindingIdentifier(bindingKey, queue);
            existingMapping = _bindingsMap.get(bindingIdentifier);

            if (existingMapping == null)
            {

                Map<String,Object> attributes = new HashMap<String, Object>();
                attributes.put(Binding.NAME,bindingKey);
                attributes.put(Binding.ID, id);
                attributes.put(Binding.ARGUMENTS, arguments);

                final BindingImpl b = new BindingImpl(attributes, queue, this);

                final SettableFuture<Boolean> returnVal = SettableFuture.create();

                addFutureCallback(b.createAsync(), new FutureCallback<Void>()
                {
                    @Override
                    public void onSuccess(final Void result)
                    {
                        try
                        {
                            addBinding(b);
                            returnVal.set(true);
                        }
                        catch(Throwable t)
                        {
                            returnVal.setException(t);
                        }
                    }

                    @Override
                    public void onFailure(final Throwable t)
                    {
                        returnVal.setException(t);
                    }
                }, getTaskExecutor()); // Must be called before addBinding as it resolves automated attributes.

                return returnVal;
            }
            else if(force)
            {
                Map<String,Object> oldArguments = existingMapping.getArguments();
                ((BindingImpl)existingMapping).setArguments(arguments);
                onBindingUpdated(existingMapping, oldArguments);
                return Futures.immediateFuture(true);
            }
            else
            {
                return Futures.immediateFuture(false);
            }
        }
    }

    @Override
    public void addBinding(final Binding<?> b)
    {
        BindingIdentifier identifier = new BindingIdentifier(b.getName(), b.getQueue());

        _bindingsMap.put(identifier, b);
        b.getQueue().addBinding(b);
        childAdded(b);

    }

    protected abstract void onBindingUpdated(final Binding<?> binding,
                                             final Map<String, Object> oldArguments);


    @StateTransition(currentState = {State.UNINITIALIZED,State.ERRORED}, desiredState = State.ACTIVE)
    private ListenableFuture<Void> activate()
    {
        setState(State.ACTIVE);
        return Futures.immediateFuture(null);
    }


    @StateTransition(currentState = State.UNINITIALIZED, desiredState = State.DELETED)
    private ListenableFuture<Void>  doDeleteBeforeInitialize()
    {
        preSetAlternateExchange();
        setState(State.DELETED);
        return Futures.immediateFuture(null);
    }


    @StateTransition(currentState = State.ACTIVE, desiredState = State.DELETED)
    private ListenableFuture<Void> doDelete()
    {
        try
        {
            ListenableFuture<Void> removeExchangeFuture = deleteWithChecks();
            return doAfter(removeExchangeFuture, new Runnable()
            {
                @Override
                public void run()
                {
                    preSetAlternateExchange();
                    setState(State.DELETED);
                }
            });
        }
        catch(ExchangeIsAlternateException | RequiredExchangeException e)
        {
            // let management know about constraint violations
            // in order to report error back to caller
            return Futures.immediateFailedFuture(e);
        }
    }

    @Override
    public <C extends ConfiguredObject> Collection<C> getChildren(final Class<C> clazz)
    {
        if(org.apache.qpid.server.model.Binding.class.isAssignableFrom(clazz))
        {

            return (Collection<C>) getBindings();
        }
        else
        {
            return Collections.EMPTY_SET;
        }
    }

    private interface BindingListener<X extends AbstractExchange<X>>
    {
        void bindingAdded(AbstractExchange<X> exchange, Binding<?> binding);
        void bindingRemoved(AbstractExchange<X> exchange, Binding<?> binding);
    }

    private static final class BindingIdentifier
    {
        private final String _bindingKey;
        private final Queue<?> _destination;

        private BindingIdentifier(final String bindingKey, final Queue<?> destination)
        {
            _bindingKey = bindingKey;
            _destination = destination;
        }

        public String getBindingKey()
        {
            return _bindingKey;
        }

        public Queue<?> getDestination()
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

            if (!_bindingKey.equals(that._bindingKey))
            {
                return false;
            }
            if (!_destination.equals(that._destination))
            {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = _bindingKey.hashCode();
            result = 31 * result + _destination.hashCode();
            return result;
        }
    }

    @Override
    public Collection<Publisher> getPublishers()
    {
        return Collections.emptySet();
    }

    // Used by the protocol layers
    @Override
    public boolean deleteBinding(final String bindingKey, final Queue<?> queue)
    {
        final Binding<?> binding = getBinding(bindingKey, queue);
        if(binding == null)
        {
            return false;
        }
        else
        {
            binding.delete();
            autoDeleteIfNecessary();
            return true;
        }
    }

    @Override
    public boolean hasBinding(final String bindingKey, final Queue<?> queue)
    {
        return getBinding(bindingKey,queue) != null;
    }

    @Override
    public org.apache.qpid.server.model.Binding createBinding(final String bindingKey,
                                                              final Queue queue,
                                                              final Map<String, Object> bindingArguments,
                                                              final Map<String, Object> attributes)
    {
        addBinding(bindingKey, (Queue<?>) queue, bindingArguments);
        final Binding<?> binding = getBinding(bindingKey, (Queue<?>) queue);
        return binding;
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
}
