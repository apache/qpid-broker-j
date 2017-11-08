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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.transport.network.security.ssl.SSLUtil;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

public class NonBlockingConnectionTLSDelegate implements NonBlockingConnectionDelegate
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NonBlockingConnectionTLSDelegate.class);

    private final SSLEngine _sslEngine;
    private final NonBlockingConnection _parent;
    private final int _networkBufferSize;
    private SSLEngineResult _status;
    private final List<QpidByteBuffer> _encryptedOutput = new ArrayList<>();
    private Principal _principal;
    private Certificate _peerCertificate;
    private boolean _principalChecked;
    private volatile boolean _hostChecked;
    private QpidByteBuffer _netInputBuffer;
    private QpidByteBuffer _netOutputBuffer;
    private QpidByteBuffer _applicationBuffer;


    public NonBlockingConnectionTLSDelegate(NonBlockingConnection parent, AmqpPort port)
    {
        _parent = parent;
        _sslEngine = createSSLEngine(port);
        _networkBufferSize = port.getNetworkBufferSize();

        final int tlsPacketBufferSize = _sslEngine.getSession().getPacketBufferSize();
        if (tlsPacketBufferSize > _networkBufferSize)
        {
            throw new ServerScopedRuntimeException("TLS implementation packet buffer size (" + tlsPacketBufferSize
                    + ") is greater then broker network buffer size (" + _networkBufferSize + ")");
        }

        _netInputBuffer = QpidByteBuffer.allocateDirect(_networkBufferSize);
        _applicationBuffer = QpidByteBuffer.allocateDirect(_networkBufferSize);
        _netOutputBuffer = QpidByteBuffer.allocateDirect(_networkBufferSize);
    }

    @Override
    public boolean readyForRead()
    {
        return _sslEngine.getHandshakeStatus() != SSLEngineResult.HandshakeStatus.NEED_WRAP;
    }

    @Override
    public boolean processData() throws IOException
    {
        if(!_hostChecked)
        {
            try (QpidByteBuffer buffer = _netInputBuffer.duplicate())
            {
                buffer.flip();
                if (SSLUtil.isSufficientToDetermineClientSNIHost(buffer))
                {
                    String hostName = SSLUtil.getServerNameFromTLSClientHello(buffer);
                    if (hostName != null)
                    {
                        _parent.setSelectedHost(hostName);
                        SSLParameters sslParameters = _sslEngine.getSSLParameters();
                        sslParameters.setServerNames(Collections.singletonList(new SNIHostName(hostName)));
                        _sslEngine.setSSLParameters(sslParameters);
                    }
                    _hostChecked = true;
                }
                else
                {
                    return false;
                }
            }
        }
        _netInputBuffer.flip();
        boolean readData = false;
        boolean tasksRun;
        int oldNetBufferPos;
        do
        {
            int oldAppBufPos = _applicationBuffer.position();
            oldNetBufferPos = _netInputBuffer.position();

            _status = QpidByteBuffer.decryptSSL(_sslEngine, _netInputBuffer, _applicationBuffer);
            if (_status.getStatus() == SSLEngineResult.Status.CLOSED)
            {
                int remaining = _netInputBuffer.remaining();
                _netInputBuffer.position(_netInputBuffer.limit());
                // We'd usually expect no more bytes to be sent following a close_notify
                LOGGER.debug("SSLEngine closed, discarded {} byte(s)", remaining);
            }

            tasksRun = runSSLEngineTasks(_status);
            _applicationBuffer.flip();
            if(_applicationBuffer.position() > oldAppBufPos)
            {
                readData = true;
            }

            _parent.processAmqpData(_applicationBuffer);

            restoreApplicationBufferForWrite();

        }
        while((_netInputBuffer.hasRemaining() && (_netInputBuffer.position()>oldNetBufferPos)) || tasksRun);

        if(_netInputBuffer.hasRemaining())
        {
            _netInputBuffer.compact();
        }
        else
        {
            _netInputBuffer.clear();
        }
        return readData;
    }

    @Override
    public WriteResult doWrite(Collection<QpidByteBuffer> buffers) throws IOException
    {
        final int bufCount = buffers.size();

        int totalConsumed = wrapBufferArray(buffers);

        boolean bufsSent = true;
        final Iterator<QpidByteBuffer> itr = buffers.iterator();
        int bufIndex = 0;
        while(itr.hasNext() && bufsSent && bufIndex++ < bufCount)
        {
            QpidByteBuffer buf = itr.next();
            bufsSent = !buf.hasRemaining();
        }

        if(!_encryptedOutput.isEmpty())
        {
            _parent.writeToTransport(_encryptedOutput);

            ListIterator<QpidByteBuffer> iter = _encryptedOutput.listIterator();
            while (iter.hasNext())
            {
                QpidByteBuffer buf = iter.next();
                if (!buf.hasRemaining())
                {
                    buf.dispose();
                    iter.remove();
                }
                else
                {
                    break;
                }
            }
        }
        return new WriteResult(bufsSent && _encryptedOutput.isEmpty(), totalConsumed);
    }

    protected void restoreApplicationBufferForWrite()
    {
        try (QpidByteBuffer oldApplicationBuffer = _applicationBuffer)
        {
            int unprocessedDataLength = _applicationBuffer.remaining();
            _applicationBuffer.limit(_applicationBuffer.capacity());
            _applicationBuffer = _applicationBuffer.slice();
            _applicationBuffer.limit(unprocessedDataLength);
        }
        if (_applicationBuffer.limit() <= _applicationBuffer.capacity() - _sslEngine.getSession().getApplicationBufferSize())
        {
            _applicationBuffer.position(_applicationBuffer.limit());
            _applicationBuffer.limit(_applicationBuffer.capacity());
        }
        else
        {
            try (QpidByteBuffer currentBuffer = _applicationBuffer)
            {
                int newBufSize;
                if (currentBuffer.capacity() < _networkBufferSize)
                {
                    newBufSize = _networkBufferSize;
                }
                else
                {
                    newBufSize = currentBuffer.capacity() + _networkBufferSize;
                    _parent.reportUnexpectedByteBufferSizeUsage();
                }

                _applicationBuffer = QpidByteBuffer.allocateDirect(newBufSize);
                _applicationBuffer.put(currentBuffer);
            }
        }

    }

    private int wrapBufferArray(Collection<QpidByteBuffer> buffers) throws SSLException
    {
        int totalConsumed = 0;
        boolean encrypted;
        do
        {
            if(_sslEngine.getHandshakeStatus() != SSLEngineResult.HandshakeStatus.NEED_UNWRAP)
            {
                if(_netOutputBuffer.remaining() < _sslEngine.getSession().getPacketBufferSize())
                {
                    if(_netOutputBuffer.position() != 0)
                    {
                        _netOutputBuffer.flip();
                        _encryptedOutput.add(_netOutputBuffer);
                    }
                    else
                    {
                        _netOutputBuffer.dispose();
                    }
                    _netOutputBuffer = QpidByteBuffer.allocateDirect(_networkBufferSize);
                }

                _status = QpidByteBuffer.encryptSSL(_sslEngine, buffers, _netOutputBuffer);
                encrypted = _status.bytesProduced() > 0;
                totalConsumed += _status.bytesConsumed();
                runSSLEngineTasks(_status);
                if(encrypted && _netOutputBuffer.remaining() < _sslEngine.getSession().getPacketBufferSize())
                {
                    _netOutputBuffer.flip();
                    _encryptedOutput.add(_netOutputBuffer);
                    _netOutputBuffer = QpidByteBuffer.allocateDirect(_networkBufferSize);
                }

            }
            else
            {
                encrypted = false;
            }

        }
        while(encrypted && _sslEngine.getHandshakeStatus() != SSLEngineResult.HandshakeStatus.NEED_UNWRAP);

        if(_netOutputBuffer.position() != 0)
        {
            final QpidByteBuffer outputBuffer = _netOutputBuffer;

            _netOutputBuffer = _netOutputBuffer.slice();

            outputBuffer.flip();
            _encryptedOutput.add(outputBuffer);

        }
        return totalConsumed;
    }

    private boolean runSSLEngineTasks(final SSLEngineResult status)
    {
        if(status.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_TASK)
        {
            Runnable task;
            while((task = _sslEngine.getDelegatedTask()) != null)
            {
                task.run();
            }

            return true;
        }

        return false;
    }

    @Override
    public Principal getPeerPrincipal()
    {
        checkPeerPrincipal();
        return _principal;
    }

    @Override
    public Certificate getPeerCertificate()
    {
        checkPeerPrincipal();
        return _peerCertificate;
    }

    @Override
    public boolean needsWork()
    {
        return _sslEngine.getHandshakeStatus() != SSLEngineResult.HandshakeStatus.NEED_UNWRAP;
    }

    private synchronized void checkPeerPrincipal()
    {
        if (!_principalChecked)
        {
            try
            {
                _principal = _sslEngine.getSession().getPeerPrincipal();
                final Certificate[] peerCertificates =
                        _sslEngine.getSession().getPeerCertificates();
                if (peerCertificates != null && peerCertificates.length > 0)
                {
                    _peerCertificate = peerCertificates[0];
                }
            }
            catch (SSLPeerUnverifiedException e)
            {
                _principal = null;
                _peerCertificate = null;
            }

            _principalChecked = true;
        }
    }

    private SSLEngine createSSLEngine(AmqpPort<?> port)
    {
        SSLEngine sslEngine = port.getSSLContext().createSSLEngine();
        sslEngine.setUseClientMode(false);
        SSLUtil.updateEnabledTlsProtocols(sslEngine, port.getTlsProtocolWhiteList(), port.getTlsProtocolBlackList());
        SSLUtil.updateEnabledCipherSuites(sslEngine, port.getTlsCipherSuiteWhiteList(), port.getTlsCipherSuiteBlackList());
        if(port.getTlsCipherSuiteWhiteList() != null && !port.getTlsCipherSuiteWhiteList().isEmpty())
        {
            SSLParameters sslParameters = sslEngine.getSSLParameters();
            sslParameters.setUseCipherSuitesOrder(true);
            sslEngine.setSSLParameters(sslParameters);
        }

        if(port.getNeedClientAuth())
        {
            sslEngine.setNeedClientAuth(true);
        }
        else if(port.getWantClientAuth())
        {
            sslEngine.setWantClientAuth(true);
        }
        return sslEngine;
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

        if (_applicationBuffer != null)
        {
            _applicationBuffer.dispose();
            _applicationBuffer = null;
        }
    }

    @Override
    public void shutdownOutput()
    {

        if (_netOutputBuffer != null)
        {
            _netOutputBuffer.dispose();
            _netOutputBuffer = null;
        }
        try
        {
            _sslEngine.closeOutbound();
            _sslEngine.closeInbound();
        }
        catch (SSLException e)
        {
            LOGGER.debug("Exception when closing SSLEngine", e);
        }

    }

    @Override
    public String getTransportInfo()
    {
        SSLSession session = _sslEngine.getSession();
        return session.getProtocol() + " ; " + session.getCipherSuite() ;
    }
}
