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

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.plugin.ProtocolEngineCreator;
import org.apache.qpid.server.plugin.ProtocolEngineCreatorComparator;
import org.apache.qpid.server.plugin.QpidServiceLoader;

public class MultiVersionProtocolEngineFactory implements ProtocolEngineFactory
{
    private static final AtomicLong ID_GENERATOR = new AtomicLong(0);
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiVersionProtocolEngineFactory.class);

    private final Broker<?> _broker;
    private final Set<Protocol> _supported;
    private final Protocol _defaultSupportedReply;
    private final AmqpPort<?> _port;
    private final Transport _transport;
    private final ProtocolEngineCreator[] _creators;
    private final ConnectionCountDecrementingTask
            _connectionCountDecrementingTask = new ConnectionCountDecrementingTask();

    public MultiVersionProtocolEngineFactory(Broker<?> broker,
                                             Set<Protocol> supportedVersions,
                                             Protocol defaultSupportedReply,
                                             AmqpPort<?> port,
                                             Transport transport)
    {
        if(defaultSupportedReply != null && !supportedVersions.contains(defaultSupportedReply))
        {
            LOGGER.warn("The configured default reply ({}) to an unsupported protocol version initiation is not"
                         + " supported on this port.  Only the following versions are supported: {}",
                         defaultSupportedReply, supportedVersions);
            defaultSupportedReply = null;
        }

        _broker = broker;
        _supported = supportedVersions;
        _defaultSupportedReply = defaultSupportedReply;
        final List<ProtocolEngineCreator> creators = new ArrayList<ProtocolEngineCreator>();
        for(ProtocolEngineCreator c : new QpidServiceLoader().instancesOf(ProtocolEngineCreator.class))
        {
            creators.add(c);
        }
        Collections.sort(creators, new ProtocolEngineCreatorComparator());
        _creators = creators.toArray(new ProtocolEngineCreator[creators.size()]);
        _port = port;
        _transport = transport;
    }

    @Override
    public MultiVersionProtocolEngine newProtocolEngine(final SocketAddress remoteSocketAddress)
    {
        if(_port.canAcceptNewConnection(remoteSocketAddress))
        {
            _port.incrementConnectionCount();
            return new MultiVersionProtocolEngine(_broker,
                                                  _supported, _defaultSupportedReply, _port, _transport,
                                                  ID_GENERATOR.getAndIncrement(),
                                                  _creators, _connectionCountDecrementingTask);
        }
        else
        {
            return null;
        }
    }

    private class ConnectionCountDecrementingTask implements Runnable
    {
        @Override
        public void run()
        {
            _port.decrementConnectionCount();
        }
    }
}
