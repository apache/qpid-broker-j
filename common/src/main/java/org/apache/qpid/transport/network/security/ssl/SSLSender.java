/*
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
package org.apache.qpid.transport.network.security.ssl;

import java.util.Collections;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLEngineResult.Status;
import javax.net.ssl.SSLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.transport.ByteBufferSender;
import org.apache.qpid.transport.SenderException;
import org.apache.qpid.transport.network.security.SSLStatus;

public class SSLSender implements ByteBufferSender
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SSLSender.class);

    private final ByteBufferSender delegate;
    private final SSLEngine engine;
    private final int sslBufSize;
    private final QpidByteBuffer netData;
    private final long timeout;
    private final SSLStatus _sslStatus;

    private String _hostname;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final ConcurrentLinkedQueue<QpidByteBuffer> _pending = new ConcurrentLinkedQueue<>();


    public SSLSender(SSLEngine engine, ByteBufferSender delegate, SSLStatus sslStatus)
    {
        this.engine = engine;
        this.delegate = delegate;
        sslBufSize = engine.getSession().getPacketBufferSize();
        netData = QpidByteBuffer.allocate(sslBufSize);
        timeout = Long.getLong("qpid.ssl_timeout", 60000);
        _sslStatus = sslStatus;
    }
    
    public void setHostname(String hostname)
    {
        _hostname = hostname;
    }

    public void close()
    {
        if (!closed.getAndSet(true))
        {
            if (engine.isOutboundDone())
            {
                return;
            }
            LOGGER.debug("Closing SSL connection");

            engine.closeOutbound();
            try
            {
                tearDownSSLConnection();
            }
            catch(Exception e)
            {
                throw new SenderException("Error closing SSL connection",e);
            }


            synchronized(_sslStatus.getSslLock())
            {
                while (!engine.isOutboundDone())
                {
                    try
                    {
                        _sslStatus.getSslLock().wait();
                    }
                    catch(InterruptedException e)
                    {
                        // pass
                    }

                }
            }
            delegate.close();
        }
    }

    private void tearDownSSLConnection() throws Exception
    {
        SSLEngineResult result = QpidByteBuffer.encryptSSL(engine,
                                                           Collections.singletonList(QpidByteBuffer.allocate(0)),
                                                           netData);
        Status status = result.getStatus();
        int read   = result.bytesProduced();
        while (status != Status.CLOSED)
        {
            if (status == Status.BUFFER_OVERFLOW)
            {
                netData.clear();
            }
            if(read > 0)
            {
                int limit = netData.limit();
                netData.limit(netData.position());
                netData.position(netData.position() - read);

                QpidByteBuffer data = netData.slice();

                netData.limit(limit);
                netData.position(netData.position() + read);

                delegate.send(data);
                flush();
            }
            result = QpidByteBuffer.encryptSSL(engine,
                                               Collections.singletonList(QpidByteBuffer.allocate(0)),
                                               netData);
            status = result.getStatus();
            read   = result.bytesProduced();
        }
    }

    public void flush()
    {
        doSend();
        delegate.flush();

    }

    @Override
    public boolean isDirectBufferPreferred()
    {
        return false;
    }

    public void send(QpidByteBuffer appData)
    {
        _pending.add(appData.duplicate());

    }

    public void doSend()
    {
        if (closed.get() && !_sslStatus.getSslErrorFlag())
        {
            throw new SenderException("SSL Sender is closed");
        }

        HandshakeStatus handshakeStatus;
        Status status;

        while(!_pending.isEmpty() && !_sslStatus.getSslErrorFlag())
        {
            int read = 0;
            try
            {
                SSLEngineResult result = QpidByteBuffer.encryptSSL(engine, _pending, netData);

                while(!_pending.isEmpty())
                {
                    QpidByteBuffer buf = _pending.peek();
                    if (buf.hasRemaining())
                    {
                        break;
                    }
                    buf.dispose();
                    _pending.poll();
                }

                read   = result.bytesProduced();
                status = result.getStatus();
                handshakeStatus = result.getHandshakeStatus();
            }
            catch(SSLException e)
            {
                // Should this set _sslError??
                throw new SenderException("SSL, Error occurred while encrypting data",e);
            }

            if(read > 0)
            {
                int limit = netData.limit();
                netData.limit(netData.position());
                netData.position(netData.position() - read);

                QpidByteBuffer data = netData.slice();

                netData.limit(limit);
                netData.position(netData.position() + read);

                delegate.send(data);
            }

            switch(status)
            {
                case CLOSED:
                    throw new SenderException("SSLEngine is closed");

                case BUFFER_OVERFLOW:
                    netData.clear();
                    continue;

                case OK:
                    break; // do nothing

                default:
                    throw new IllegalStateException("SSLReceiver: Invalid State " + status);
            }

            switch (handshakeStatus)
            {
                case NEED_WRAP:
                    if (netData.hasRemaining())
                    {
                        continue;
                    }
                    break;
                case NEED_TASK:
                    doTasks();
                    break;

                case NEED_UNWRAP:
                    delegate.flush();
                    synchronized(_sslStatus.getSslLock())
                    {
                        if (_sslStatus.getSslErrorFlag())
                        {
                            break;
                        }

                        switch (engine.getHandshakeStatus())
                        {
                            case NEED_UNWRAP:
                                final long start = System.currentTimeMillis();
                                try
                                {
                                    _sslStatus.getSslLock().wait(timeout);
                                }
                                catch(InterruptedException e)
                                {
                                    // pass
                                }

                                if (!_sslStatus.getSslErrorFlag() && System.currentTimeMillis() - start >= timeout)
                                {
                                    throw new SenderException(
                                            "SSL Engine timed out after waiting " + timeout + "ms. for a response." +
                                            "To get more info,run with -Djavax.net.debug=ssl");
                                }
                                break;
                        }
                    }
                    break;

                case FINISHED:
                    if (_hostname != null)
                    {
                        SSLUtil.verifyHostname(engine, _hostname);
                    }
                    break;
                case NOT_HANDSHAKING:
                    break; //do  nothing

                default:
                    throw new IllegalStateException("SSLSender: Invalid State " + status);
            }

        }
    }


    private void doTasks()
    {
        Runnable runnable;
        while ((runnable = engine.getDelegatedTask()) != null) {
            runnable.run();
        }
    }

}
