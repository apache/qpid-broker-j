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
package org.apache.qpid.server.store.berkeleydb;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Collections;

import com.sleepycat.je.LockTimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.internal.InternalMessageMetaData;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTableFactory;
import org.apache.qpid.server.protocol.v0_8.MessageMetaData;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.apache.qpid.server.store.MessageHandle;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.MessageStoreTestCase;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.server.virtualhost.berkeleydb.BDBVirtualHost;
import org.apache.qpid.test.utils.VirtualHostNodeStoreType;

/**
 * Subclass of MessageStoreTestCase which runs the standard tests from the superclass against
 * the BDB Store as well as additional tests specific to the BDB store-implementation.
 */
public class BDBMessageStoreTest extends MessageStoreTestCase
{
    private static final byte[] CONTENT_BYTES = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

    private String _storeLocation;

    @Override
    @BeforeEach
    public void setUp() throws Exception
    {
        assumeTrue(is(equalTo(VirtualHostNodeStoreType.BDB)).matches(VirtualHostNodeStoreType.BDB),
                "VirtualHostNodeStoreType should be BDB");
        super.setUp();
    }

    @Override
    @AfterEach
    public void tearDown() throws Exception
    {
        try
        {
            super.tearDown();
        }
        finally
        {
            deleteStoreIfExists();
        }
    }

    private MessagePublishInfo createPublishInfoBody_0_8()
    {
        return new MessagePublishInfo(AMQShortString.createAMQShortString("exchange12345"), false, true,
                                      AMQShortString.createAMQShortString("routingKey12345"));

    }

    private ContentHeaderBody createContentHeaderBody_0_8(BasicContentHeaderProperties props, int length)
    {
        return new ContentHeaderBody(props, length);
    }

    private BasicContentHeaderProperties createContentHeaderProperties_0_8()
    {
        BasicContentHeaderProperties props = new BasicContentHeaderProperties();
        props.setDeliveryMode(Integer.valueOf(BasicContentHeaderProperties.PERSISTENT).byteValue());
        props.setContentType("text/html");
        props.setHeaders(FieldTableFactory.createFieldTable(Collections.singletonMap("Test", "MST")));
        return props;
    }

    /**
     * Tests that messages which are added to the store and then removed using the
     * public MessageStore interfaces are actually removed from the store by then
     * interrogating the store with its own implementation methods and verifying
     * expected exceptions are thrown to indicate the message is not present.
     */
    @Test
    public void testMessageCreationAndRemoval()
    {
        BDBMessageStore bdbStore = (BDBMessageStore) getStore();

        StoredMessage<MessageMetaData> storedMessage_0_8 = createAndStoreSingleChunkMessage_0_8(bdbStore);
        long messageid_0_8 = storedMessage_0_8.getMessageNumber();

        bdbStore.removeMessage(messageid_0_8);

        //verify the removal using the BDB store implementation methods directly
        try
        {
            // the next line should throw since the message id should not be found
            bdbStore.getMessageMetaData(messageid_0_8);
            fail("No exception thrown when message id not found getting metadata");
        }
        catch (StoreException e)
        {
            // pass since exception expected
        }

        try
        {
            bdbStore.getAllContent(messageid_0_8);
            fail("Expected exception not thrown");
        }
        catch (StoreException se)
        {
            // PASS
        }
    }

    private StoredMessage<MessageMetaData> createAndStoreSingleChunkMessage_0_8(MessageStore store)
    {
        QpidByteBuffer chunk1 = QpidByteBuffer.wrap(CONTENT_BYTES);

        int bodySize = CONTENT_BYTES.length;

        //create and store the message using the MessageStore interface
        MessagePublishInfo pubInfoBody_0_8 = createPublishInfoBody_0_8();
        BasicContentHeaderProperties props_0_8 = createContentHeaderProperties_0_8();

        ContentHeaderBody chb_0_8 = createContentHeaderBody_0_8(props_0_8, bodySize);

        MessageMetaData messageMetaData_0_8 = new MessageMetaData(pubInfoBody_0_8, chb_0_8);
        MessageHandle<MessageMetaData> storedMessage_0_8 = store.addMessage(messageMetaData_0_8);

        storedMessage_0_8.addContent(chunk1);
        ((AbstractBDBMessageStore.StoredBDBMessage)storedMessage_0_8).flushToStore();

        return storedMessage_0_8.allContentAdded();
    }

    @Test
    public void testOnDelete()
    {
        String storeLocation = getStore().getStoreLocation();

        File location = new File(storeLocation);
        assertTrue(location.exists(), "Store does not exist at " + storeLocation);

        getStore().closeMessageStore();
        assertTrue(location.exists(), "Store does not exist at " + storeLocation);

        BDBVirtualHost mockVH = mock(BDBVirtualHost.class);
        String testLocation = getStore().getStoreLocation();
        when(mockVH.getStorePath()).thenReturn(testLocation);

        getStore().onDelete(mockVH);

        assertFalse(location.exists(), "Store exists at " + storeLocation);
    }


    @Override
    protected VirtualHost createVirtualHost()
    {
        _storeLocation = TMP_FOLDER + File.separator + getTestName();
        deleteStoreIfExists();

        final BDBVirtualHost parent = mock(BDBVirtualHost.class);
        when(parent.getStorePath()).thenReturn(_storeLocation);
        return parent;
    }

    private void deleteStoreIfExists()
    {
        if (_storeLocation != null)
        {
            File location = new File(_storeLocation);
            if (location.exists())
            {
                FileUtils.delete(location, true);
            }
        }
    }

    @Override
    protected MessageStore createMessageStore()
    {
        return new BDBMessageStore();
    }

    @Override
    protected boolean flowToDiskSupported()
    {
        return true;
    }

    @Test
    public void testGetNextMessgaeIdWithLockException(){
        EnvironmentFacadeFactory eff = mock(EnvironmentFacadeFactory.class);
        EnvironmentFacade ef = mock(EnvironmentFacade.class);
        doThrow(LockTimeoutException.class).when(ef).openSequence(any(),any(),any());
        when(eff.createEnvironmentFacade(any())).thenReturn(ef);

        BDBMessageStore store = new BDBMessageStore (eff);
        store.openMessageStore(getVirtualHost());

        try
        {
            store.addMessage(new InternalMessageMetaData(false, null, 0));
            fail("exception is expected");
        }
        catch(ConnectionScopedRuntimeException e)
        {
            assertEquals("Unexpected exception on BDB sequence", e.getMessage());
        }

    }
}
