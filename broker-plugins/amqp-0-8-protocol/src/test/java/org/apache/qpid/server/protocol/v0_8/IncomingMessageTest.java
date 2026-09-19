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
package org.apache.qpid.server.protocol.v0_8;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.apache.qpid.test.utils.UnitTestBase;

class IncomingMessageTest extends UnitTestBase
{
    @Test
    void emptyContentBodyFramesAreCountedButNotRetained()
    {
        final IncomingMessage message = createMessage(1L);

        try (final QpidByteBuffer emptyContent = QpidByteBuffer.wrap(new byte[0]))
        {
            for (int i = 0; i < 100_000; i++)
            {
                message.addContentBodyFrame(emptyContent);
            }
        }

        assertEquals(100_000, message.getContentBodyFrameCount());
        assertEquals(0, message.getContentChunkCount());
        assertFalse(message.allContentReceived());
        message.getContentHeader().dispose();
    }

    @Test
    void nonEmptyContentBodyFrameIsRetainedOnce()
    {
        final IncomingMessage message = createMessage(1L);

        try (final QpidByteBuffer content = QpidByteBuffer.wrap(new byte[]{42}))
        {
            assertTrue(message.addContentBodyFrame(content));
        }

        assertEquals(1, message.getContentBodyFrameCount());
        assertEquals(1, message.getContentChunkCount());
        assertTrue(message.allContentReceived());

        final QpidByteBuffer retainedContent = message.getContentChunk(0);
        assertEquals((byte) 42, retainedContent.get());
        retainedContent.dispose();
        message.getContentHeader().dispose();
    }

    @Test
    void contentBodyFrameExceedingDeclaredSizeIsNotRetained()
    {
        final IncomingMessage message = createMessage(1L);

        try (final QpidByteBuffer content = QpidByteBuffer.wrap(new byte[2]))
        {
            assertFalse(message.addContentBodyFrame(content));
        }

        assertEquals(0, message.getContentBodyFrameCount());
        assertEquals(0, message.getContentChunkCount());
        assertFalse(message.allContentReceived());
        message.getContentHeader().dispose();
    }

    @Test
    void disposeReleasesHeaderAndRetainedContent()
    {
        final IncomingMessage message = new IncomingMessage(mock(MessagePublishInfo.class));
        final ContentHeaderBody contentHeader = mock(ContentHeaderBody.class);
        when(contentHeader.getBodySize()).thenReturn(1L);
        message.setContentHeaderBody(contentHeader);
        final QpidByteBuffer content = mock(QpidByteBuffer.class);
        final QpidByteBuffer retainedContent = mock(QpidByteBuffer.class);
        when(content.remaining()).thenReturn(1);
        when(content.duplicate()).thenReturn(retainedContent);

        assertTrue(message.addContentBodyFrame(content));

        message.dispose();
        message.dispose();

        verify(contentHeader).dispose();
        verify(retainedContent).dispose();
        assertNull(message.getContentHeader());
        assertEquals(0, message.getContentChunkCount());
    }

    private IncomingMessage createMessage(final long bodySize)
    {
        final IncomingMessage message = new IncomingMessage(mock(MessagePublishInfo.class));
        message.setContentHeaderBody(new ContentHeaderBody(new BasicContentHeaderProperties(), bodySize));
        return message;
    }
}
