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
package org.apache.qpid.server.message;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

public abstract class AbstractServerMessageImpl<X extends AbstractServerMessageImpl<X,T>, T extends StorableMessageMetaData> implements ServerMessage<T>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractServerMessageImpl.class);
    private static final AtomicIntegerFieldUpdater<AbstractServerMessageImpl> _refCountUpdater =
            AtomicIntegerFieldUpdater.newUpdater(AbstractServerMessageImpl.class, "_referenceCount");

    private static final AtomicReferenceFieldUpdater<AbstractServerMessageImpl, Collection> _resourcesUpdater =
            AtomicReferenceFieldUpdater.newUpdater(AbstractServerMessageImpl.class, Collection.class,"_resources");

    private volatile int _referenceCount = 0;
    private final StoredMessage<T> _handle;
    private final Object _connectionReference;
    @SuppressWarnings("unused")
    private volatile Collection<UUID> _resources;

    private volatile ServerMessage.ValidationStatus _validationStatus = ServerMessage.ValidationStatus.UNKNOWN;

    private static final AtomicReferenceFieldUpdater<AbstractServerMessageImpl, ServerMessage.ValidationStatus>
            _validationStatusUpdater = AtomicReferenceFieldUpdater.newUpdater(AbstractServerMessageImpl.class,
                                                                              ServerMessage.ValidationStatus.class,
                                                                              "_validationStatus");

    public AbstractServerMessageImpl(StoredMessage<T> handle, Object connectionReference)
    {
        _handle = handle;
        _connectionReference = connectionReference;
    }

    @Override
    public long getSize()
    {
        return _handle.getContentSize();
    }

    @Override
    public long getSizeIncludingHeader()
    {
        return _handle.getContentSize() + _handle.getMetadataSize();
    }

    @Override
    public StoredMessage<T> getStoredMessage()
    {
        return _handle;
    }

    private boolean incrementReference()
    {
        do
        {
            int count = _refCountUpdater.get(this);

            if (count < 0)
            {
                return false;
            }
            else if (_refCountUpdater.compareAndSet(this, count, count + 1))
            {
                return true;
            }
        }
        while (true);
    }

    /**
     * Thread-safe. This will decrement the reference count and when it reaches zero will remove the message from the
     * message store.
     *
     */
    private void decrementReference()
    {
        boolean updated;
        do
        {
            int count = _refCountUpdater.get(this);
            int newCount = count - 1;

            if (newCount > 0)
            {
                updated = _refCountUpdater.compareAndSet(this, count, newCount);
            }
            else if (newCount == 0)
            {
                // set the reference count below 0 so that we can detect that the message has been deleted
                updated = _refCountUpdater.compareAndSet(this, count, -1);
                if (updated)
                {
                    _handle.remove();
                }
            }
            else
            {
                throw new ServerScopedRuntimeException("Reference count for message id " + debugIdentity()
                                                       + " has gone below 0.");
            }
        }
        while (!updated);
    }

    public String debugIdentity()
    {
        return "(HC:" + System.identityHashCode(this) + " ID:" + getMessageNumber() + " Ref:" + getReferenceCount() + ")";
    }

    private int getReferenceCount()
    {
        return _referenceCount;
    }

    @Override
    final public MessageReference<X> newReference()
    {
        return new Reference(this);
    }

    @Override
    final public MessageReference<X> newReference(TransactionLogResource object)
    {
        return new Reference(this, object);
    }

    @Override
    final public boolean isReferenced(TransactionLogResource resource)
    {
        Collection<UUID> resources = _resources;
        return resources != null && resources.contains(resource.getId());
    }

    @Override
    final public boolean isReferenced()
    {
        Collection<UUID> resources = _resources;
        return resources != null && !resources.isEmpty();
    }

    @Override
    final public boolean isPersistent()
    {
        return _handle.getMetaData().isPersistent();
    }

    @Override
    final public long getMessageNumber()
    {
        return getStoredMessage().getMessageNumber();
    }

    @Override
    public QpidByteBuffer getContent()
    {
        return getContent(0, (int) getSize());
    }

    @Override
    public QpidByteBuffer getContent(int offset, int length)
    {
        StoredMessage<T> storedMessage = getStoredMessage();
        boolean wasInMemory = storedMessage.isInContentInMemory();
        try
        {
            return storedMessage.getContent(offset, length);
        }
        finally
        {
            if (!wasInMemory && checkValid())
            {
                storedMessage.flowToDisk();
            }
        }
    }

    @Override
    final public Object getConnectionReference()
    {
        return _connectionReference;
    }

    @Override
    public String toString()
    {
        return "Message[" + debugIdentity() + "]";
    }

    @Override
    public ServerMessage.ValidationStatus getValidationStatus()
    {
        return _validationStatus;
    }

    @Override
    public boolean checkValid()
    {
        ServerMessage.ValidationStatus status;
        while ((status = _validationStatus) == ServerMessage.ValidationStatus.UNKNOWN)
        {
            ServerMessage.ValidationStatus newStatus;
            try
            {
                validate();
                newStatus = ServerMessage.ValidationStatus.VALID;
            }
            catch (RuntimeException e)
            {
                newStatus = ServerMessage.ValidationStatus.MALFORMED;
                LOGGER.debug("Malformed message '{}' detected", this, e);
            }

            if (_validationStatusUpdater.compareAndSet(this, status, newStatus))
            {
                status = newStatus;
                break;
            }
        }
        return status == ServerMessage.ValidationStatus.VALID;
    }

    protected void validate()
    {
        // noop
    }

    private static class Reference<X extends AbstractServerMessageImpl<X,T>, T extends StorableMessageMetaData>
            implements MessageReference<X>
    {

        private static final AtomicIntegerFieldUpdater<Reference> _releasedUpdater =
                AtomicIntegerFieldUpdater.newUpdater(Reference.class, "_released");

        private final AbstractServerMessageImpl<X, T> _message;
        private final UUID _resourceId;
        private volatile int _released;

        private Reference(final AbstractServerMessageImpl<X, T> message) throws MessageDeletedException
        {
            this(message, null);
        }
        private Reference(final AbstractServerMessageImpl<X, T> message, TransactionLogResource resource) throws MessageDeletedException
        {
            _message = message;
            if(resource != null)
            {
                Collection<UUID> currentValue;
                Collection<UUID> newValue;
                _resourceId = resource.getId();
                do
                {
                    currentValue = _message._resources;

                    if(currentValue == null)
                    {
                        newValue = Collections.singleton(_resourceId);
                    }
                    else
                    {
                        if(currentValue.contains(_resourceId))
                        {
                            throw new MessageAlreadyReferencedException(_message.getMessageNumber(), resource);
                        }
                        newValue = new ArrayList<>(currentValue.size()+1);
                        newValue.addAll(currentValue);
                        newValue.add(_resourceId);
                    }

                }
                while(!_resourcesUpdater.compareAndSet(_message, currentValue, newValue));
            }
            else
            {
                _resourceId = null;
            }
            if(!_message.incrementReference())
            {
                throw new MessageDeletedException(message.getMessageNumber());
            }

        }

        @Override
        public X getMessage()
        {
            return (X) _message;
        }

        @Override
        public synchronized void release()
        {
            if(_releasedUpdater.compareAndSet(this,0,1))
            {
                if(_resourceId != null)
                {
                    Collection<UUID> currentValue;
                    Collection<UUID> newValue;
                    do
                    {
                        currentValue = _message._resources;
                        if(currentValue.size() == 1)
                        {
                            newValue = null;
                        }
                        else
                        {
                            UUID[] array = new UUID[currentValue.size()-1];
                            int pos = 0;
                            for(UUID uuid : currentValue)
                            {
                                if(!_resourceId.equals(uuid))
                                {
                                    array[pos++] = uuid;
                                }
                            }
                            newValue = Arrays.asList(array);
                        }
                    }
                    while(!_resourcesUpdater.compareAndSet(_message, currentValue, newValue));

                }
                _message.decrementReference();
            }
        }

        @Override
        public void close()
        {
            release();
        }
    }

}
