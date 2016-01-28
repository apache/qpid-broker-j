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
package org.apache.qpid.transport.network.security.sasl;


import java.util.concurrent.atomic.AtomicBoolean;

import javax.security.sasl.SaslException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.transport.ByteBufferSender;
import org.apache.qpid.transport.SenderException;

public class SASLSender extends SASLEncryptor implements ByteBufferSender
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SASLSender.class);

    private ByteBufferSender delegate;
    private byte[] appData;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public SASLSender(ByteBufferSender delegate)
    {
        this.delegate = delegate;
        LOGGER.debug("SASL Sender enabled");
    }

    public void close() 
    {
        
        if (!closed.getAndSet(true))
        {
            delegate.close();
            if (isSecurityLayerEstablished())
            {
                try
                {
                    getSaslClient().dispose();
                } 
                catch (SaslException e)
                {
                    throw new SenderException("Error closing SASL Sender",e);
                }
            }
        }
    }

    public void flush() 
    {
       delegate.flush();
    }

    @Override
    public boolean isDirectBufferPreferred()
    {
        return false;
    }

    public void send(QpidByteBuffer buf)
    {
        if (closed.get())
        {
            throw new SenderException("SSL Sender is closed");
        }
        buf = buf.duplicate();
        if (isSecurityLayerEstablished())
        {
            while (buf.hasRemaining())
            {
                int length = Math.min(buf.remaining(), getSendBuffSize());
                LOGGER.debug("sendBuffSize {}", getSendBuffSize());
                LOGGER.debug("buf.remaining() {}", buf.remaining());

                buf.get(appData, 0, length);
                try
                {
                    byte[] out = getSaslClient().wrap(appData, 0, length);
                    LOGGER.debug("out.length {}", out.length);

                    delegate.send(QpidByteBuffer.wrap(out));
                }
                catch (SaslException e)
                {
                    LOGGER.error("Exception while encrypting data.", e);
                    throw new SenderException("SASL Sender, Error occurred while encrypting data",e);
                }
            }
        }
        else
        {
            delegate.send(buf);
        }
        buf.dispose();
    }


    public void securityLayerEstablished()
    {
        appData = new byte[getSendBuffSize()];
        LOGGER.debug("SASL Security Layer Established");
    }

}
