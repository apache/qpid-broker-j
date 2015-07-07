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
package org.apache.qpid.server.transport;


import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.security.cert.Certificate;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.logging.messages.ConnectionMessages;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.plugin.ProtocolEngineCreator;
import org.apache.qpid.server.security.ManagedPeerCertificateTrustStore;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.transport.ByteBufferSender;
import org.apache.qpid.transport.network.AggregateTicker;
import org.apache.qpid.transport.network.NetworkConnection;

public class MultiVersionProtocolEngine implements ProtocolEngine
{
    private static final Logger _logger = LoggerFactory.getLogger(MultiVersionProtocolEngine.class);

    private final long _id;
    private final AmqpPort<?> _port;
    private Transport _transport;
    private final ProtocolEngineCreator[] _creators;
    private final Runnable _onCloseTask;

    private Set<Protocol> _supported;
    private String _fqdn;
    private final Broker<?> _broker;
    private NetworkConnection _network;
    private ByteBufferSender _sender;
    private final Protocol _defaultSupportedReply;

    private volatile ProtocolEngine _delegate = new SelfDelegateProtocolEngine();
    private final AtomicReference<Action<ProtocolEngine>> _workListener = new AtomicReference<>();
    private final AggregateTicker _aggregateTicker = new AggregateTicker();

    public MultiVersionProtocolEngine(final Broker<?> broker,
                                      final Set<Protocol> supported,
                                      Protocol defaultSupportedReply,
                                      AmqpPort<?> port,
                                      Transport transport,
                                      final long id,
                                      ProtocolEngineCreator[] creators,
                                      final Runnable onCloseTask)
    {
        _id = id;
        _broker = broker;
        _supported = supported;
        _defaultSupportedReply = defaultSupportedReply;
        _port = port;
        _transport = transport;
        _creators = creators;
        _onCloseTask = onCloseTask;
    }

    @Override
    public void setMessageAssignmentSuspended(final boolean value)
    {
        _delegate.setMessageAssignmentSuspended(value);
    }

    @Override
    public boolean isMessageAssignmentSuspended()
    {
        return _delegate.isMessageAssignmentSuspended();
    }

    public void closed()
    {
        _logger.debug("Closed");

        try
        {
            _delegate.closed();
        }
        finally
        {
            if(_onCloseTask != null)
            {
                _onCloseTask.run();
            }
        }
    }

    public void writerIdle()
    {
        _delegate.writerIdle();
    }

    public void readerIdle()
    {
        _delegate.readerIdle();
    }

    @Override
    public void encryptedTransport()
    {
        _delegate.encryptedTransport();
    }


    public void received(ByteBuffer msg)
    {
        _delegate.received(msg);
    }

    public long getConnectionId()
    {
        return _id;
    }

    @Override
    public Subject getSubject()
    {
        return _delegate.getSubject();
    }

    @Override
    public boolean isTransportBlockedForWriting()
    {
        return _delegate.isTransportBlockedForWriting();
    }

    @Override
    public void setTransportBlockedForWriting(final boolean blocked)
    {
        _delegate.setTransportBlockedForWriting(blocked);
    }

    private static final int MINIMUM_REQUIRED_HEADER_BYTES = 8;

    public void setNetworkConnection(NetworkConnection network)
    {
        _network = network;
        SocketAddress address = _network.getLocalAddress();
        if (address instanceof InetSocketAddress)
        {
            _fqdn = ((InetSocketAddress) address).getHostName();
        }
        else
        {
            throw new IllegalArgumentException("Unsupported socket address class: " + address);
        }
        _sender = network.getSender();
    }

    @Override
    public long getLastReadTime()
    {
        return _delegate.getLastReadTime();
    }

    @Override
    public long getLastWriteTime()
    {
        return _delegate.getLastWriteTime();
    }

    @Override
    public void processPending()
    {
        _delegate.processPending();
    }

    @Override
    public boolean hasWork()
    {
        return _delegate.hasWork();
    }

    @Override
    public void notifyWork()
    {
        _delegate.notifyWork();
    }

    @Override
    public void setWorkListener(final Action<ProtocolEngine> listener)
    {
        _workListener.set(listener);
        _delegate.setWorkListener(listener);
    }

    @Override
    public void clearWork()
    {
        _delegate.clearWork();
    }

    @Override
    public AggregateTicker getAggregateTicker()
    {
        return _aggregateTicker;
    }

    private class ClosedDelegateProtocolEngine implements ProtocolEngine
    {

        @Override
        public void setMessageAssignmentSuspended(final boolean value)
        {

        }

        @Override
        public boolean isMessageAssignmentSuspended()
        {
            return false;
        }

        @Override
        public void processPending()
        {

        }

        @Override
        public boolean hasWork()
        {
            return false;
        }

        @Override
        public void notifyWork()
        {

        }

        @Override
        public void setWorkListener(final Action<ProtocolEngine> listener)
        {

        }

        @Override
        public void clearWork()
        {

        }

        public void received(ByteBuffer msg)
        {
            _logger.error("Error processing incoming data, could not negotiate a common protocol");
        }

        public void closed()
        {

        }

        public void writerIdle()
        {

        }

        public void readerIdle()
        {

        }

        @Override
        public void encryptedTransport()
        {

        }

        @Override
        public long getLastReadTime()
        {
            return 0;
        }

        @Override
        public long getLastWriteTime()
        {
            return 0;
        }

        @Override
        public Subject getSubject()
        {
            return new Subject();
        }

        @Override
        public boolean isTransportBlockedForWriting()
        {
            return false;
        }

        @Override
        public void setTransportBlockedForWriting(final boolean blocked)
        {
        }

        @Override
        public AggregateTicker getAggregateTicker()
        {
            return _aggregateTicker;
        }

    }

    private class SelfDelegateProtocolEngine implements ProtocolEngine
    {
        private final ByteBuffer _header = ByteBuffer.allocate(MINIMUM_REQUIRED_HEADER_BYTES);
        private long _lastReadTime = System.currentTimeMillis();
        private final AtomicBoolean _hasWork = new AtomicBoolean();

        @Override
        public void setMessageAssignmentSuspended(final boolean value)
        {
        }

        @Override
        public boolean isMessageAssignmentSuspended()
        {
            return false;
        }

        @Override
        public void processPending()
        {

        }

        @Override
        public boolean hasWork()
        {
            return _hasWork.get();
        }

        @Override
        public void notifyWork()
        {
            _hasWork.set(true);
        }

        @Override
        public void setWorkListener(final Action<ProtocolEngine> listener)
        {

        }

        @Override
        public AggregateTicker getAggregateTicker()
        {
            return _aggregateTicker;
        }

        @Override
        public void clearWork()
        {
            _hasWork.set(false);
        }

        public void received(ByteBuffer msg)
        {
            _lastReadTime = System.currentTimeMillis();
            ByteBuffer msgheader = msg.duplicate().slice();

            if(_header.remaining() > msgheader.limit())
            {
                msg.position(msg.limit());
            }
            else
            {
                msgheader.limit(_header.remaining());
                msg.position(msg.position()+_header.remaining());
            }

            _header.put(msgheader);

            if(!_header.hasRemaining())
            {
                _header.flip();
                byte[] headerBytes = new byte[MINIMUM_REQUIRED_HEADER_BYTES];
                _header.get(headerBytes);


                ProtocolEngine newDelegate = null;
                byte[] supportedReplyBytes = null;
                byte[] defaultSupportedReplyBytes = null;
                Protocol supportedReplyVersion = null;

                //Check the supported versions for a header match, and if there is one save the
                //delegate. Also save most recent supported version and associated reply header bytes
                for(int i = 0; newDelegate == null && i < _creators.length; i++)
                {
                    if(_supported.contains(_creators[i].getVersion()))
                    {
                        supportedReplyBytes = _creators[i].getHeaderIdentifier();
                        supportedReplyVersion = _creators[i].getVersion();
                        byte[] compareBytes = _creators[i].getHeaderIdentifier();
                        boolean equal = true;
                        for(int j = 0; equal && j<compareBytes.length; j++)
                        {
                            equal = headerBytes[j] == compareBytes[j];
                        }
                        if(equal)
                        {
                            newDelegate = _creators[i].newProtocolEngine(_broker,
                                                                         _network, _port, _transport, _id,
                                                                         _aggregateTicker);
                            if(newDelegate == null && _creators[i].getSuggestedAlternativeHeader() != null)
                            {
                                defaultSupportedReplyBytes = _creators[i].getSuggestedAlternativeHeader();
                            }
                        }
                    }

                    //If there is a configured default reply to an unsupported version initiation,
                    //then save the associated reply header bytes when we encounter them
                    if(defaultSupportedReplyBytes == null && _defaultSupportedReply != null && _creators[i].getVersion() == _defaultSupportedReply)
                    {
                        defaultSupportedReplyBytes = _creators[i].getHeaderIdentifier();
                    }
                }

                // If no delegate is found then send back a supported protocol version id
                if(newDelegate == null)
                {
                    //if a default reply was specified use its reply header instead of the most recent supported version
                    if(_defaultSupportedReply != null && !(_defaultSupportedReply == supportedReplyVersion))
                    {
                        if(_logger.isDebugEnabled())
                        {
                            _logger.debug("Default reply to unsupported protocol version was configured, changing reply from "
                                    + supportedReplyVersion + " to " + _defaultSupportedReply);
                        }

                        supportedReplyBytes = defaultSupportedReplyBytes;
                        supportedReplyVersion = _defaultSupportedReply;
                    }
                    if(_logger.isDebugEnabled())
                    {
                        _logger.debug("Unsupported protocol version requested, replying with: " + supportedReplyVersion);
                    }
                    _sender.send(ByteBuffer.wrap(supportedReplyBytes));
                    _sender.flush();

                    _delegate = new ClosedDelegateProtocolEngine();

                    _network.close();

                }
                else
                {
                    boolean hasWork = _delegate.hasWork();
                    if (hasWork)
                    {
                        newDelegate.notifyWork();
                    }
                    _delegate = newDelegate;
                    _delegate.setWorkListener(_workListener.get());
                    _header.flip();
                    _delegate.received(_header);

                    Certificate peerCertificate = _network.getPeerCertificate();
                    if(peerCertificate != null && _port.getClientCertRecorder() != null)
                    {
                        ((ManagedPeerCertificateTrustStore)(_port.getClientCertRecorder())).addCertificate(peerCertificate);
                    }


                    if(msg.hasRemaining())
                    {
                        _delegate.received(msg);
                    }
                }

            }



        }


        @Override
        public Subject getSubject()
        {
            return _delegate.getSubject();
        }

        @Override
        public boolean isTransportBlockedForWriting()
        {
            return false;
        }

        @Override
        public void setTransportBlockedForWriting(final boolean blocked)
        {
        }

        public void closed()
        {
            try
            {
                _delegate = new ClosedDelegateProtocolEngine();
                if(_logger.isDebugEnabled())
                {
                    _logger.debug("Connection from  " + _network.getRemoteAddress() + " was closed before any protocol version was established.");
                }
            }
            catch(Exception e)
            {
                //ignore
            }
            finally
            {
                try
                {
                    _network.close();
                }
                catch(Exception e)
                {
                    //ignore
                }
            }
        }

        public void writerIdle()
        {

        }

        public void readerIdle()
        {
            _broker.getEventLogger().message(ConnectionMessages.IDLE_CLOSE());
            _network.close();
        }

        @Override
        public void encryptedTransport()
        {
            if(_transport == Transport.TCP)
            {
                _transport = Transport.SSL;
            }
        }

        @Override
        public long getLastReadTime()
        {
            return _lastReadTime;
        }

        @Override
        public long getLastWriteTime()
        {
            return 0;
        }
    }


}
