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
package org.apache.qpid.server.protocol.v1_0.store;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.MessageMetaData_1_0;
import org.apache.qpid.server.protocol.v1_0.type.messaging.ApplicationProperties;
import org.apache.qpid.server.protocol.v1_0.type.messaging.ApplicationPropertiesSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Header;
import org.apache.qpid.server.protocol.v1_0.type.messaging.PropertiesSection;
import org.apache.qpid.server.store.MessageHandle;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.test.utils.UnitTestBase;

@Isolated("Changes the global QpidByteBuffer pool")
public abstract class FlowToDiskMetaDataTestCase extends UnitTestBase
{
    private static final byte[] PROPERTIES = HexFormat.of().parseHex("00800000000000000073d00000000400000000");
    private static final byte[] APPLICATION_PROPERTIES = HexFormat.of()
            .parseHex("00800000000000000074d10000001000000002b1000000016bb10000000176");
    private static final byte[] BODY = HexFormat.of().parseHex("005375b00000000401020304");

    private MessageStore _store;
    private int _buffersInUse;

    protected abstract MessageStore createMessageStore();

    @BeforeEach
    public void setUp()
    {
        QpidByteBuffer.deinitialisePool();
        _buffersInUse = QpidByteBuffer.getNumberOfBuffersInUse();
        QpidByteBuffer.initialisePool(256 * 1024, 16, 0.5);
        _store = createMessageStore();
    }

    @AfterEach
    public void tearDown()
    {
        try
        {
            if (_store != null)
            {
                _store.closeMessageStore();
            }
        }
        finally
        {
            QpidByteBuffer.deinitialisePool();
            assertEquals(_buffersInUse, QpidByteBuffer.getNumberOfBuffersInUse());
        }
    }

    @Test
    public void heldMetaDataDoesNotLeakAfterFlowToDisk()
    {
        final MessageMetaData_1_0 metaData = new MessageMetaData_1_0(new Header().createEncodingRetainingSection(),
                null, null, null, new ApplicationProperties(Map.of("key", "value")).createEncodingRetainingSection(),
                null, System.currentTimeMillis(), 0);
        final StoredMessage<MessageMetaData_1_0> message = _store.addMessage(metaData).allContentAdded();
        final MessageMetaData_1_0 heldMetaData = message.getMetaData();
        try
        {
            assertTrue(message.flowToDisk());
            final ApplicationPropertiesSection section = heldMetaData.getApplicationPropertiesSection();
            section.dispose();
            message.remove();
            assertNoPooledBuffersRetained();
        }
        finally
        {
            heldMetaData.dispose();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void mixedMetaDataDoesNotLeakAfterFlowToDisk(final boolean firstFragmentDirect)
    {
        final StoredMessage<MessageMetaData_1_0> message;
        try (final QpidByteBuffer properties = mixedEncoding(PROPERTIES, firstFragmentDirect);
             final QpidByteBuffer applicationProperties = mixedEncoding(APPLICATION_PROPERTIES, firstFragmentDirect))
        {
            message = addEncodedMessage(properties, applicationProperties);
        }
        final MessageMetaData_1_0 heldMetaData = message.getMetaData();
        try
        {
            assertTrue(message.flowToDisk());
            assertEquals(0, message.getInMemorySize());
            assertNoPooledBuffersRetained();

            QpidByteBuffer.initialisePool(256 * 1024, 16, 0.5);
            assertMessageEncoding(heldMetaData);
            heldMetaData.reallocate();
            heldMetaData.clearEncodedForm();
            assertMessageEncoding(heldMetaData);
            message.remove();
            assertNoPooledBuffersRetained();
        }
        finally
        {
            heldMetaData.dispose();
        }
    }

    @Test
    public void repeatedEvictionPreservesHeldMetadataAndStoredBytes()
    {
        final StoredMessage<MessageMetaData_1_0> message = addEncodedMessage();
        final List<MessageMetaData_1_0> heldMetadata = new ArrayList<>();
        try
        {
            for (int i = 0; i < 20; i++)
            {
                final MessageMetaData_1_0 metadata = message.getMetaData();
                heldMetadata.add(metadata);
                assertTrue(message.flowToDisk());
                assertEquals(0, message.getInMemorySize());
                assertMessageEncoding(metadata);
                assertEquals("v", metadata.getMessageHeader().getHeader("k"));
                assertNotSame(metadata, message.getMetaData());
                assertMessageEncoding(message.getMetaData());
                try (final QpidByteBuffer content = message.getContent(0, BODY.length))
                {
                    assertTrue(message.flowToDisk());
                    assertArrayEquals(BODY, readBytes(content));
                }
            }
            message.remove();
            for (final MessageMetaData_1_0 metadata : heldMetadata)
            {
                assertMessageEncoding(metadata);
            }
            assertNoPooledBuffersRetained();
        }
        finally
        {
            heldMetadata.forEach(MessageMetaData_1_0::dispose);
        }
    }

    @Test
    public void readersOverlapStoreEviction() throws Exception
    {
        final StoredMessage<MessageMetaData_1_0> message = addEncodedMessage();
        final CountDownLatch workersStopped = new CountDownLatch(2);
        final ThreadFactory threadFactory = Executors.defaultThreadFactory();
        final ExecutorService executor = Executors.newFixedThreadPool(2, QpidByteBuffer
                .createQpidByteBufferTrackingThreadFactory(task -> threadFactory.newThread(() ->
                {
                    try
                    {
                        task.run();
                    }
                    finally
                    {
                        workersStopped.countDown();
                    }
                })));
        final CountDownLatch metadataHeld = new CountDownLatch(1);
        final CountDownLatch evicted = new CountDownLatch(1);
        try
        {
            final Future<?> reader = executor.submit(() ->
            {
                final MessageMetaData_1_0 metadata = message.getMetaData();
                metadataHeld.countDown();
                assertTrue(evicted.await(10, TimeUnit.SECONDS));
                for (int i = 0; i < 100; i++)
                {
                    assertMessageEncoding(metadata);
                }
                return null;
            });
            final Future<?> eviction = executor.submit(() ->
            {
                assertTrue(metadataHeld.await(10, TimeUnit.SECONDS));
                assertTrue(message.flowToDisk());
                evicted.countDown();
                for (int i = 0; i < 100; i++)
                {
                    message.getMetaData();
                    assertTrue(message.flowToDisk());
                }
                return null;
            });
            reader.get(30, TimeUnit.SECONDS);
            eviction.get(30, TimeUnit.SECONDS);
            message.remove();
        }
        finally
        {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
            assertTrue(workersStopped.await(10, TimeUnit.SECONDS));
        }
        assertNoPooledBuffersRetained();
    }

    private StoredMessage<MessageMetaData_1_0> addEncodedMessage()
    {
        try (final QpidByteBuffer properties = QpidByteBuffer.allocateDirect(PROPERTIES.length);
             final QpidByteBuffer applicationProperties = QpidByteBuffer.allocateDirect(APPLICATION_PROPERTIES.length))
        {
            properties.put(PROPERTIES).flip();
            applicationProperties.put(APPLICATION_PROPERTIES).flip();
            return addEncodedMessage(properties, applicationProperties);
        }
    }

    private StoredMessage<MessageMetaData_1_0> addEncodedMessage(final QpidByteBuffer properties,
                                                                 final QpidByteBuffer applicationProperties)
    {
        try (final QpidByteBuffer body = QpidByteBuffer.allocateDirect(BODY.length))
        {
            body.put(BODY).flip();
            final MessageMetaData_1_0 metadata = new MessageMetaData_1_0(null, null, null,
                    new PropertiesSection(properties), new ApplicationPropertiesSection(applicationProperties),
                    null, System.currentTimeMillis(), BODY.length);
            final MessageHandle<MessageMetaData_1_0> handle = _store.addMessage(metadata);
            handle.addContent(body);
            return handle.allContentAdded();
        }
    }

    private QpidByteBuffer mixedEncoding(final byte[] bytes, final boolean firstFragmentDirect)
    {
        final int split = bytes.length / 2;
        try (final QpidByteBuffer first = QpidByteBuffer.allocate(firstFragmentDirect, split);
             final QpidByteBuffer second = QpidByteBuffer.allocate(!firstFragmentDirect, bytes.length - split))
        {
            first.put(bytes, 0, split).flip();
            second.put(bytes, split, bytes.length - split).flip();
            return QpidByteBuffer.concatenate(first, second);
        }
    }

    private void assertMessageEncoding(final MessageMetaData_1_0 metadata)
    {
        final PropertiesSection properties = metadata.getPropertiesSection();
        final ApplicationPropertiesSection applicationProperties = metadata.getApplicationPropertiesSection();
        try (final QpidByteBuffer propertiesBytes = properties.getEncodedForm();
             final QpidByteBuffer applicationBytes = applicationProperties.getEncodedForm())
        {
            assertArrayEquals(PROPERTIES, readBytes(propertiesBytes));
            assertArrayEquals(APPLICATION_PROPERTIES, readBytes(applicationBytes));
            assertEquals(17 + PROPERTIES.length + APPLICATION_PROPERTIES.length, metadata.getStorableSize());
        }
        finally
        {
            properties.dispose();
            applicationProperties.dispose();
        }
    }

    private byte[] readBytes(final QpidByteBuffer buffer)
    {
        final byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    private void assertNoPooledBuffersRetained()
    {
        QpidByteBuffer.deinitialisePool();
        assertEquals(_buffersInUse, QpidByteBuffer.getNumberOfBuffersInUse(),
                "Metadata released by the store must not retain pooled buffers");
    }
}
