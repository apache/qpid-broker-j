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
package org.apache.qpid.server.message.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.AbstractServerMessageImpl;
import org.apache.qpid.server.store.MessageHandle;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.util.ByteBufferInputStream;
import org.apache.qpid.util.ByteBufferUtils;

public class InternalMessage extends AbstractServerMessageImpl<InternalMessage, InternalMessageMetaData>
{
    private static final String NON_AMQP_MESSAGE = "Non-AMQP Message";
    private final Object _messageBody;
    private InternalMessageHeader _header;
    private String _initialRoutingAddress;


    InternalMessage(final StoredMessage<InternalMessageMetaData> handle,
                final InternalMessageHeader header,
                final Object messageBody)
    {
        super(handle, null, handle.getMetaData().getContentSize());
        _header = header;
        _messageBody = messageBody;
    }

    InternalMessage(final StoredMessage<InternalMessageMetaData> msg)
    {
        super(msg, null, msg.getMetaData().getContentSize());
        _header = msg.getMetaData().getHeader();
        Collection<QpidByteBuffer> bufs = msg.getContent(0, (int)getSize());

        try(ObjectInputStream is = new ObjectInputStream(new ByteBufferInputStream(ByteBufferUtils.combine(bufs))))
        {
            _messageBody = is.readObject();

        }
        catch (IOException e)
        {
            throw new ConnectionScopedRuntimeException("Unexpected IO Exception in operation in memory", e);
        }
        catch (ClassNotFoundException e)
        {
            throw new ConnectionScopedRuntimeException("Object message contained an object which could not " +
                                                       "be deserialized", e);
        }
    }

    @Override
    public String getInitialRoutingAddress()
    {
        return _initialRoutingAddress;
    }

    @Override
    public InternalMessageHeader getMessageHeader()
    {
        return _header;
    }

    @Override
    public long getExpiration()
    {
        return _header.getExpiration();
    }

    @Override
    public String getMessageType()
    {
        return NON_AMQP_MESSAGE;
    }

    @Override
    public long getArrivalTime()
    {
        return _header.getArrivalTime();
    }

    @Override
    public boolean isResourceAcceptable(final TransactionLogResource resource)
    {
        return true;
    }

    public Object getMessageBody()
    {
        return _messageBody;
    }

    public static InternalMessage createMessage(final MessageStore store,
                                                final AMQMessageHeader header,
                                                final Serializable bodyObject, final boolean persistent)
    {
        InternalMessageHeader internalHeader;
        if(header instanceof InternalMessageHeader)
        {
            internalHeader = (InternalMessageHeader) header;
        }
        else
        {
            internalHeader = new InternalMessageHeader(header);
        }
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        try (ObjectOutputStream os = new ObjectOutputStream(bytesOut))
        {
            os.writeObject(bodyObject);
            os.close();
            byte[] bytes = bytesOut.toByteArray();


            final InternalMessageMetaData metaData = InternalMessageMetaData.create(persistent, internalHeader, bytes.length);
            MessageHandle<InternalMessageMetaData> handle = store.addMessage(metaData);
            handle.addContent(QpidByteBuffer.wrap(bytes));
            StoredMessage<InternalMessageMetaData> storedMessage = handle.allContentAdded();
            return new InternalMessage(storedMessage, internalHeader, bodyObject);
        }
        catch (IOException e)
        {
            throw new ConnectionScopedRuntimeException("Unexpected IO Exception on operation in memory", e);
        }
    }

    public static InternalMessage createStringMessage(MessageStore store, AMQMessageHeader header, String messageBody)
    {
        return createStringMessage(store, header, messageBody, false);
    }


    public static InternalMessage createStringMessage(MessageStore store, AMQMessageHeader header, String messageBody, boolean persistent)
    {
        return createMessage(store, header, messageBody, persistent);
    }

    public static InternalMessage createMapMessage(MessageStore store, AMQMessageHeader header, Map<? extends Object,? extends Object> messageBody)
    {
        return createMessage(store, header, new LinkedHashMap<Object,Object>(messageBody), false);
    }

    public static InternalMessage createListMessage(MessageStore store, AMQMessageHeader header, List<? extends Object> messageBody)
    {
        return createMessage(store, header, new ArrayList<Object>(messageBody), false);
    }

    public static InternalMessage createBytesMessage(MessageStore store, AMQMessageHeader header, byte[] messageBody)
    {
        return createBytesMessage(store, header, messageBody, false);
    }


    public static InternalMessage createBytesMessage(MessageStore store, AMQMessageHeader header, byte[] messageBody, boolean persist)
    {
        return createMessage(store, header, messageBody, persist);
    }

    public static InternalMessage convert(long messageNumber, boolean persistent, AMQMessageHeader header, Object messageBody)
    {
        InternalMessageHeader convertedHeader = new InternalMessageHeader(header);
        StoredMessage<InternalMessageMetaData> handle = createReadOnlyHandle(messageNumber, persistent, convertedHeader, messageBody);
        return new InternalMessage(handle, convertedHeader, messageBody);
    }

    private static StoredMessage<InternalMessageMetaData> createReadOnlyHandle(final long messageNumber,
                                                                               final boolean persistent,
                                                                               final InternalMessageHeader header,
                                                                               final Object messageBody)
    {


        try(ByteArrayOutputStream bytesOut = new ByteArrayOutputStream())
        {
            try(ObjectOutputStream os = new ObjectOutputStream(bytesOut))
            {
                os.writeObject(messageBody);
                final byte[] bytes = bytesOut.toByteArray();


                final InternalMessageMetaData metaData =
                        InternalMessageMetaData.create(persistent, header, bytes.length);


                return new StoredMessage<InternalMessageMetaData>()
                {
                    @Override
                    public InternalMessageMetaData getMetaData()
                    {
                        return metaData;
                    }

                    @Override
                    public long getMessageNumber()
                    {
                        return messageNumber;
                    }

                    @Override
                    public Collection<QpidByteBuffer> getContent(final int offset, final int length)
                    {
                        return Collections.singleton(QpidByteBuffer.wrap(bytes, offset, length));
                    }

                    @Override
                    public void remove()
                    {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public boolean isInMemory()
                    {
                        return true;
                    }

                    @Override
                    public boolean flowToDisk()
                    {
                        return false;
                    }
                };
            }
        }
        catch (IOException e)
        {
            throw new ConnectionScopedRuntimeException("Unexpected IO Exception on operation in memory", e);
        }
    }


    public void setInitialRoutingAddress(final String initialRoutingAddress)
    {
        _initialRoutingAddress = initialRoutingAddress;
    }
}
