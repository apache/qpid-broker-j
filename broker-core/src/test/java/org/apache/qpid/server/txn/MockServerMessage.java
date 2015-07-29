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
package org.apache.qpid.server.txn;

import java.nio.ByteBuffer;
import java.util.Collection;


import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.TransactionLogResource;

/**
 * Mock Server Message allowing its persistent flag to be controlled from test.
 */
class MockServerMessage implements ServerMessage
{
    /**
     * 
     */
    private final boolean persistent;

    /**
     * @param persistent
     */
    MockServerMessage(boolean persistent)
    {
        this.persistent = persistent;
    }

    public boolean isPersistent()
    {
        return persistent;
    }

    public MessageReference newReference()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MessageReference newReference(final TransactionLogResource object)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isReferenced(final TransactionLogResource resource)
    {
        return false;
    }

    @Override
    public boolean isReferenced()
    {
        return false;
    }

    public long getSize()
    {
        throw new UnsupportedOperationException();
    }

    public String getInitialRoutingAddress()
    {
        throw new UnsupportedOperationException();
    }

    public AMQMessageHeader getMessageHeader()
    {
        throw new UnsupportedOperationException();
    }

    public StoredMessage getStoredMessage()
    {
        throw new UnsupportedOperationException();
    }

    public long getExpiration()
    {
        throw new UnsupportedOperationException();
    }

    public int getContent(ByteBuffer buf, int offset)
    {
        throw new UnsupportedOperationException();
    }


    public Collection<ByteBuffer> getContent(int offset, int size)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getConnectionReference()
    {
        return null;
    }

    public long getArrivalTime()
    {
        throw new UnsupportedOperationException();
    }

    public long getMessageNumber()
    {
        return 0L;
    }
}
