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


import java.nio.ByteBuffer;

import javax.security.sasl.SaslException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.transport.ExceptionHandlingByteBufferReceiver;
import org.apache.qpid.transport.SenderException;

public class SASLReceiver extends SASLEncryptor implements ExceptionHandlingByteBufferReceiver
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SASLReceiver.class);

    private ExceptionHandlingByteBufferReceiver delegate;
    private byte[] netData;

    public SASLReceiver(ExceptionHandlingByteBufferReceiver delegate)
    {
        this.delegate = delegate;
    }
    
    public void closed() 
    {
        delegate.closed();
    }


    public void exception(Throwable t) 
    {
        delegate.exception(t);
    }

    public void received(ByteBuffer buf)
    {
        if (isSecurityLayerEstablished())
        {
            while (buf.hasRemaining())
            {
                int length = Math.min(buf.remaining(), getRecvBuffSize());
                buf.get(netData, 0, length);
                try
                {
                    byte[] out = getSaslClient().unwrap(netData, 0, length);
                    delegate.received(ByteBuffer.wrap(out));
                } 
                catch (SaslException e)
                {
                    throw new SenderException("SASL Sender, Error occurred while encrypting data",e);
                }
            }            
        }
        else
        {
            delegate.received(buf);
        }        
    }
    
    public void securityLayerEstablished()
    {
        netData = new byte[getRecvBuffSize()];
        LOGGER.debug("SASL Security Layer Established");
    }

}
