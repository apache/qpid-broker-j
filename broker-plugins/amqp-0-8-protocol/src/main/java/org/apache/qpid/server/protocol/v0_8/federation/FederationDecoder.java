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
package org.apache.qpid.server.protocol.v0_8.federation;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.codec.ClientDecoder;
import org.apache.qpid.framing.AMQFrameDecodingException;
import org.apache.qpid.framing.AMQProtocolVersionException;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

class FederationDecoder extends ClientDecoder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FederationDecoder.class);


    private final OutboundConnection_0_8 _connection;

    FederationDecoder(final OutboundConnection_0_8 connection)
    {
        super(connection);
        _connection = connection;
    }

    public void decodeBuffer(QpidByteBuffer buf) throws AMQFrameDecodingException, AMQProtocolVersionException, IOException
    {
        decode(buf);
    }

    @Override
    protected void processFrame(final int channelId, final byte type, final long bodySize, final QpidByteBuffer in)
            throws AMQFrameDecodingException
    {
        long startTime = 0;
        try
        {
            if (LOGGER.isDebugEnabled())
            {
                startTime = System.currentTimeMillis();
            }
            OutboundChannel channel = _connection.getChannel(channelId);
            if(channel == null)
            {
                doProcessFrame(channelId, type, bodySize, in);
            }
            else
            {

                try
                {
                    AccessController.doPrivileged(new PrivilegedExceptionAction<Object>()
                    {
                        @Override
                        public Void run() throws IOException, AMQFrameDecodingException
                        {
                            doProcessFrame(channelId, type, bodySize, in);
                            return null;
                        }
                    }, channel.getAccessControllerContext());
                }
                catch (PrivilegedActionException e)
                {
                    Throwable cause = e.getCause();
                    if(cause instanceof AMQFrameDecodingException)
                    {
                        throw (AMQFrameDecodingException) cause;
                    }
                    else if(cause instanceof RuntimeException)
                    {
                        throw (RuntimeException) cause;
                    }
                    else
                    {
                        throw new ServerScopedRuntimeException(cause);
                    }
                }
            }
        }
        finally
        {
            if(LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Frame handled in {} ms.", (System.currentTimeMillis() - startTime));
            }
        }
    }


    private void doProcessFrame(final int channelId, final byte type, final long bodySize, final QpidByteBuffer in)
            throws AMQFrameDecodingException
    {
        super.processFrame(channelId, type, bodySize, in);

    }


}
