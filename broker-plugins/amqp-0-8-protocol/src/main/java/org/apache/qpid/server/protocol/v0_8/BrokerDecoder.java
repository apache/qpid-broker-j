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

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

public class BrokerDecoder extends ServerDecoder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerDecoder.class);
    private final AMQPConnection_0_8Impl _connection;
    /**
     * Creates a new AMQP decoder.
     *
     * @param connection
     */
    public BrokerDecoder(final AMQPConnection_0_8Impl connection)
    {
        super(connection);
        _connection = connection;
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
            AMQChannel channel = _connection.getChannel(channelId);
            if(channel != null)
            {
                _connection.channelRequiresSync(channel);
            }
            doProcessFrame(channelId, type, bodySize, in);

        }
        finally
        {
            if(LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Frame handled in {} ms.", (System.currentTimeMillis() - startTime));
            }
        }
    }

    @Override
    protected int processAMQPFrames(final QpidByteBuffer buf) throws AMQFrameDecodingException
    {
        int required = decodable(buf);
        if (required == 0)
        {
            final int channelId = buf.getUnsignedShort(buf.position() + 1);
            final AMQChannel channel = _connection.getChannel(channelId);

            if (channel == null)
            {
                processInput(buf);
                return 0;
            }

            else
            {
                try
                {
                    return AccessController.doPrivileged(new PrivilegedExceptionAction<Integer>()
                    {
                        @Override
                        public Integer run() throws IOException, AMQFrameDecodingException
                        {
                            int required;
                            while (true)
                            {
                                processInput(buf);

                                required = decodable(buf);
                                if (required != 0 || buf.getUnsignedShort(buf.position() + 1) != channelId)
                                {
                                    break;
                                }
                            }

                            return required;
                        }
                    }, channel.getAccessControllerContext());
                }
                catch (PrivilegedActionException e)
                {
                    Throwable cause = e.getCause();
                    if (cause instanceof AMQFrameDecodingException)
                    {
                        throw (AMQFrameDecodingException) cause;
                    }
                    else if (cause instanceof RuntimeException)
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
        return required;
    }

    private void doProcessFrame(final int channelId, final byte type, final long bodySize, final QpidByteBuffer in)
            throws AMQFrameDecodingException
    {
        super.processFrame(channelId, type, bodySize, in);

    }

}
