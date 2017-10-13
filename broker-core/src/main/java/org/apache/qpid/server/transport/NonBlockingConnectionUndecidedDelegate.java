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
 */

package org.apache.qpid.server.transport;

import java.io.IOException;
import java.security.Principal;
import java.security.cert.Certificate;
import java.util.Collection;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.transport.network.TransportEncryption;

public class NonBlockingConnectionUndecidedDelegate implements NonBlockingConnectionDelegate
{
    private static final int NUMBER_OF_BYTES_FOR_TLS_CHECK = 6;
    public final NonBlockingConnection _parent;

    private QpidByteBuffer _netInputBuffer;

    public NonBlockingConnectionUndecidedDelegate(NonBlockingConnection parent)
    {
        _parent = parent;
        _netInputBuffer = QpidByteBuffer.allocateDirect(NUMBER_OF_BYTES_FOR_TLS_CHECK);

    }

    @Override
    public boolean readyForRead()
    {
        return true;
    }

    @Override
    public boolean processData() throws IOException
    {
        try (QpidByteBuffer buffer = _netInputBuffer.duplicate())
        {
            buffer.flip();
            final boolean hasSufficientData = buffer.remaining() >= NUMBER_OF_BYTES_FOR_TLS_CHECK;
            if (hasSufficientData)
            {
                final byte[] headerBytes = new byte[NUMBER_OF_BYTES_FOR_TLS_CHECK];
                buffer.get(headerBytes);

                if (looksLikeSSL(headerBytes))
                {
                    _parent.setTransportEncryption(TransportEncryption.TLS);
                }
                else
                {
                    _parent.setTransportEncryption(TransportEncryption.NONE);
                }
            }
            return hasSufficientData;
        }
    }

    @Override
    public WriteResult doWrite(Collection<QpidByteBuffer> buffers) throws IOException
    {
        return new WriteResult(true, 0L);
    }

    @Override
    public Principal getPeerPrincipal()
    {
        return null;
    }

    @Override
    public Certificate getPeerCertificate()
    {
        return null;
    }

    @Override
    public boolean needsWork()
    {
        return false;
    }

    private boolean looksLikeSSL(final byte[] headerBytes)
    {
        return looksLikeSSLv3ClientHello(headerBytes) || looksLikeSSLv2ClientHello(headerBytes);
    }

    private boolean looksLikeSSLv3ClientHello(final byte[] headerBytes)
    {
        return headerBytes[0] == 22 && // SSL Handshake
                (headerBytes[1] == 3 && // SSL 3.0 / TLS 1.x
                        (headerBytes[2] == 0 || // SSL 3.0
                                headerBytes[2] == 1 || // TLS 1.0
                                headerBytes[2] == 2 || // TLS 1.1
                                headerBytes[2] == 3)) && // TLS1.2
                (headerBytes[5] == 1); // client_hello
    }

    private boolean looksLikeSSLv2ClientHello(final byte[] headerBytes)
    {
        return headerBytes[0] == -128 &&
                headerBytes[3] == 3 && // SSL 3.0 / TLS 1.x
                (headerBytes[4] == 0 || // SSL 3.0
                        headerBytes[4] == 1 || // TLS 1.0
                        headerBytes[4] == 2 || // TLS 1.1
                        headerBytes[4] == 3);
    }

    @Override
    public QpidByteBuffer getNetInputBuffer()
    {
        return _netInputBuffer;
    }

    @Override
    public void shutdownInput()
    {
        if (_netInputBuffer != null)
        {
            _netInputBuffer.dispose();
            _netInputBuffer = null;
        }
    }

    @Override
    public void shutdownOutput()
    {
    }

    @Override
    public String getTransportInfo()
    {
        return "";
    }
}
