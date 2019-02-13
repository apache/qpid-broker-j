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
package org.apache.qpid.server.store.serializer.v1;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.EnqueueableMessage;
import org.apache.qpid.server.plugin.MessageMetaDataType;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.store.MessageDurability;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.MessageHandle;
import org.apache.qpid.server.store.MessageMetaDataTypeRegistry;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.Transaction;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.store.handler.DistributedTransactionHandler;
import org.apache.qpid.server.store.handler.MessageHandler;
import org.apache.qpid.server.store.handler.MessageInstanceHandler;
import org.apache.qpid.server.store.serializer.MessageStoreSerializer;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

@PluggableService
public class MessageStoreSerializer_v1 implements MessageStoreSerializer
{

    public static final String VERSION = "v1.0";

    @Override
    public String getType()
    {
        return VERSION;
    }

    @Override
    public void serialize(final Map<UUID, String> queueMap,
                          final MessageStore.MessageStoreReader storeReader,
                          final OutputStream outputStream)
            throws IOException
    {
        final Serializer serializer = new Serializer(outputStream);

        serializeQueueMappings(queueMap, serializer);

        serializeMessages(storeReader, serializer);

        serializeMessageInstances(storeReader, serializer);

        serializeDistributedTransactions(storeReader, serializer);

        serializer.complete();
    }


    private void serializeQueueMappings(final Map<UUID, String> queueMap, final Serializer serializer)
            throws IOException
    {
        for (Map.Entry<UUID, String> entry : queueMap.entrySet())
        {
            serializer.add(new QueueMappingRecord(entry.getKey(), entry.getValue()));
        }
    }

    private void serializeMessages(final MessageStore.MessageStoreReader storeReader, final Serializer serializer)
            throws IOException
    {
        SerializerMessageHandler messageHandler = new SerializerMessageHandler(serializer);

        storeReader.visitMessages(messageHandler);
        if (messageHandler.getException() != null)
        {
            throw messageHandler.getException();
        }
    }

    private void serializeMessageInstances(final MessageStore.MessageStoreReader storeReader,
                                           final Serializer serializer) throws IOException
    {
        SerializerMessageInstanceHandler messageInstanceHandler = new SerializerMessageInstanceHandler(serializer);
        storeReader.visitMessageInstances(messageInstanceHandler);
        if (messageInstanceHandler.getException() != null)
        {
            throw messageInstanceHandler.getException();
        }
    }

    private void serializeDistributedTransactions(final MessageStore.MessageStoreReader storeReader,
                                                  final Serializer serializer) throws IOException
    {
        SerializerDistributedTransactionHandler distributedTransactionHandler =
                new SerializerDistributedTransactionHandler(serializer);
        storeReader.visitDistributedTransactions(distributedTransactionHandler);
        if (distributedTransactionHandler.getException() != null)
        {
            throw distributedTransactionHandler.getException();
        }
    }


    @Override
    public void deserialize(final Map<String, UUID> queueMap, final MessageStore store, final InputStream inputStream) throws IOException
    {
        final Deserializer deserializer = new Deserializer(inputStream);

        Map<Long, StoredMessage<?>> messageMap = new HashMap<>();
        Map<UUID, UUID> queueIdMap = new HashMap<>();

        Record nextRecord = deserializer.readRecord();
        switch(nextRecord.getType())
        {
            case VERSION:
                break;
            default:
                throw new IllegalArgumentException("Unexpected record type: " + nextRecord.getType() + " expecting VERSION");
        }

        nextRecord = deserializer.readRecord();

        nextRecord = deserializeQueueMappings(queueMap, queueIdMap, deserializer, nextRecord);

        nextRecord = deserializeMessages(messageMap, store, deserializer, nextRecord);

        nextRecord = deserializeMessageInstances(store, queueIdMap, messageMap, deserializer, nextRecord);

        nextRecord = deserializeDistributedTransactions(store, queueIdMap, messageMap, deserializer, nextRecord);

        if(nextRecord.getType() != RecordType.DIGEST)
        {
            throw new IllegalArgumentException("Unexpected record type '"+nextRecord.getType()+"' expecting DIGEST");
        }

    }

    private Record deserializeDistributedTransactions(final MessageStore store,
                                                      final Map<UUID, UUID> queueIdMap,
                                                      final Map<Long, StoredMessage<?>> messageMap,
                                                      final Deserializer deserializer,
                                                      Record nextRecord) throws IOException
    {
        while(nextRecord.getType() == RecordType.DTX)
        {
            DTXRecord dtxRecord = (DTXRecord)nextRecord;

            final Transaction txn = store.newTransaction();
            Transaction.StoredXidRecord xid = dtxRecord.getXid();
            final Transaction.EnqueueRecord[] translatedEnqueues = translateEnqueueRecords(dtxRecord.getEnqueues(), queueIdMap, messageMap);
            final Transaction.DequeueRecord[] translatedDequeues = translateDequeueRecords(dtxRecord.getDequeues(), queueIdMap, messageMap);
            txn.recordXid(xid.getFormat(), xid.getGlobalId(), xid.getBranchId(), translatedEnqueues, translatedDequeues);
            txn.commitTranAsync(null);
            nextRecord = deserializer.readRecord();
        }
        return nextRecord;
    }

    private Transaction.DequeueRecord[] translateDequeueRecords(final Transaction.DequeueRecord[] dequeues,
                                                                final Map<UUID, UUID> queueIdMap,
                                                                final Map<Long, StoredMessage<?>> messageMap)
    {
        Transaction.DequeueRecord[] translatedRecords = new Transaction.DequeueRecord[dequeues.length];
        for(int i = 0; i < dequeues.length; i++)
        {
            translatedRecords[i] = new DTXRecord.DequeueRecordImpl(messageMap.get(dequeues[i].getEnqueueRecord().getMessageNumber()).getMessageNumber(), queueIdMap.get(dequeues[i].getEnqueueRecord().getQueueId()));
        }
        return translatedRecords;
    }

    private Transaction.EnqueueRecord[] translateEnqueueRecords(final Transaction.EnqueueRecord[] enqueues,
                                                                final Map<UUID, UUID> queueIdMap,
                                                                final Map<Long, StoredMessage<?>> messageMap)
    {
        Transaction.EnqueueRecord[] translatedRecords = new Transaction.EnqueueRecord[enqueues.length];
        for(int i = 0; i < enqueues.length; i++)
        {
            translatedRecords[i] = new DTXRecord.EnqueueRecordImpl(messageMap.get(enqueues[i].getMessage().getMessageNumber()).getMessageNumber(), queueIdMap.get(enqueues[i].getResource().getId()));
        }
        return translatedRecords;
    }

    private Record deserializeMessageInstances(final MessageStore store,
                                               final Map<UUID, UUID> queueIdMap,
                                               final Map<Long, StoredMessage<?>> messageMap,
                                               final Deserializer deserializer,
                                               Record nextRecord)
            throws IOException
    {
        while(nextRecord.getType() == RecordType.MESSAGE_INSTANCE)
        {
            MessageInstanceRecord messageInstanceRecord = (MessageInstanceRecord) nextRecord;
            final StoredMessage<?> storedMessage = messageMap.get(messageInstanceRecord.getMessageNumber());
            final UUID queueId = queueIdMap.get(messageInstanceRecord.getQueueId());
            if(storedMessage != null && queueId != null)
            {
                final Transaction txn = store.newTransaction();

                EnqueueableMessage msg = new EnqueueableMessage()
                {
                    @Override
                    public long getMessageNumber()
                    {
                        return storedMessage.getMessageNumber();
                    }

                    @Override
                    public boolean isPersistent()
                    {
                        return true;
                    }

                    @Override
                    public StoredMessage getStoredMessage()
                    {
                        return storedMessage;
                    }
                };

                txn.enqueueMessage(new TransactionLogResource()
                {
                    @Override
                    public String getName()
                    {
                        return queueId.toString();
                    }

                    @Override
                    public UUID getId()
                    {
                        return queueId;
                    }

                    @Override
                    public MessageDurability getMessageDurability()
                    {
                        return MessageDurability.DEFAULT;
                    }
                }, msg);

                txn.commitTranAsync(null);
            }
            nextRecord = deserializer.readRecord();
        }
        return nextRecord;
    }


    private Record deserializeQueueMappings(final Map<String, UUID> queueMap,
                                            final Map<UUID, UUID> queueIdMap,
                                            final Deserializer deserializer,
                                            Record record) throws IOException
    {
        while(record.getType() == RecordType.QUEUE_MAPPING)
        {
            QueueMappingRecord queueMappingRecord = (QueueMappingRecord) record;

            if(queueMap.containsKey(queueMappingRecord.getName()))
            {
                queueIdMap.put(queueMappingRecord.getId(), queueMap.get(queueMappingRecord.getName()));
            }
            else
            {
                throw new IllegalArgumentException("The message store expects the existence of a queue named '"+queueMappingRecord.getName()+"'");
            }

            record = deserializer.readRecord();
        }
        return record;
    }


    private Record deserializeMessages(final Map<Long, StoredMessage<?>> messageNumberMap,
                                       final MessageStore store,
                                       final Deserializer deserializer,
                                       Record record)
            throws IOException
    {
        while(record.getType() == RecordType.MESSAGE)
        {
            MessageRecord messageRecord = (MessageRecord) record;
            long originalMessageNumber = messageRecord.getMessageNumber();
            byte[] metaData = messageRecord.getMetaData();
            final MessageMetaDataType metaDataType = MessageMetaDataTypeRegistry.fromOrdinal(metaData[0] & 0xff);
            final MessageHandle<StorableMessageMetaData> handle;
            try (QpidByteBuffer buf = QpidByteBuffer.wrap(metaData, 1, metaData.length - 1))
            {
                try
                {
                    StorableMessageMetaData storableMessageMetaData = metaDataType.createMetaData(buf);
                    handle = store.addMessage(storableMessageMetaData);
                }
                catch (ConnectionScopedRuntimeException e)
                {
                    throw new IllegalArgumentException("Could not deserialize message metadata", e);
                }
            }

            try (QpidByteBuffer buf = QpidByteBuffer.wrap(messageRecord.getContent()))
            {
                handle.addContent(buf);
            }
            final StoredMessage<StorableMessageMetaData> storedMessage = handle.allContentAdded();
            try
            {
                storedMessage.flowToDisk();
                messageNumberMap.put(originalMessageNumber, storedMessage);
            }
            catch (RuntimeException e)
            {
                if (e instanceof ServerScopedRuntimeException)
                {
                    throw e;
                }
                throw new IllegalArgumentException("Could not decode message metadata", e);
            }

            record = deserializer.readRecord();
        }
        return record;
    }


    private static class SerializerMessageHandler implements MessageHandler
    {
        private final Serializer _serializer;
        private IOException _exception;

        public SerializerMessageHandler(final Serializer serializer)
        {
            _serializer = serializer;
        }

        @Override
        public boolean handle(final StoredMessage<?> storedMessage)
        {
            try
            {
                _serializer.add(new MessageRecord(storedMessage));
            }
            catch (IOException e)
            {
                _exception = e;
                return false;
            }
            return true;
        }

        public IOException getException()
        {
            return _exception;
        }
    }

    private static class SerializerMessageInstanceHandler implements MessageInstanceHandler
    {
        private final Serializer _serializer;
        private IOException _exception;

        private SerializerMessageInstanceHandler(final Serializer serializer)
        {
            _serializer = serializer;
        }

        @Override
        public boolean handle(final MessageEnqueueRecord record)
        {
            try
            {
                _serializer.add(new MessageInstanceRecord(record));
            }
            catch (IOException e)
            {
                _exception = e;
                return false;
            }
            return true;
        }


        public IOException getException()
        {
            return _exception;
        }
    }

    private static class SerializerDistributedTransactionHandler implements DistributedTransactionHandler
    {
        private final Serializer _serializer;
        private IOException _exception;

        public SerializerDistributedTransactionHandler(final Serializer serializer)
        {
            _serializer = serializer;
        }

        @Override
        public boolean handle(final Transaction.StoredXidRecord storedXid,
                              final Transaction.EnqueueRecord[] enqueues,
                              final Transaction.DequeueRecord[] dequeues)
        {
            try
            {
                _serializer.add(new DTXRecord(storedXid, enqueues, dequeues));
            }
            catch (IOException e)
            {
                _exception = e;
                return false;
            }
            return true;
        }

        public IOException getException()
        {
            return _exception;
        }

    }
}
