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
package org.apache.qpid.tests.protocol.v0_8;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionOpenBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionStartOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionTuneOkBody;

public class ConnectionInteraction
{
    private final Interaction _interaction;

    private Map<String, Object> _startOkClientProperties = new HashMap<>();
    private String _startOkMechanism;
    private byte[] _startOkResponse;
    private String _startOkLocale;
    private int _tuneOkChannelMax;
    private long _tuneOkFrameMax;
    private int _tuneOkHeartbeat;
    private String _openVirtualHost;

    public ConnectionInteraction(final Interaction interaction)
    {
        _interaction = interaction;
    }


    public ConnectionInteraction startOkMechanism(final String startOkMechanism)
    {
        _startOkMechanism = startOkMechanism;
        return this;
    }

    public ConnectionInteraction startOkClientProperties(final Map<String, Object> clientProperties)
    {
        _startOkClientProperties = clientProperties == null ? Collections.emptyMap() : new HashMap<>(clientProperties);
        return this;
    }

    public Interaction startOk() throws Exception
    {
        return _interaction.sendPerformative(new ConnectionStartOkBody(FieldTable.convertToFieldTable(_startOkClientProperties),
                                                                       AMQShortString.valueOf(_startOkMechanism),
                                                                       _startOkResponse,
                                                                       AMQShortString.valueOf(_startOkLocale)));
    }

    public ConnectionInteraction tuneOkChannelMax(final int channelMax)
    {
        _tuneOkChannelMax = channelMax;
        return this;
    }

    public ConnectionInteraction tuneOkFrameMax(final long frameMax)
    {
        _tuneOkFrameMax = frameMax;
        return this;
    }

    public ConnectionInteraction tuneOkHeartbeat(final int heartbeat)
    {
        _tuneOkHeartbeat = heartbeat;
        return this;
    }

    public Interaction tuneOk() throws Exception
    {
        return _interaction.sendPerformative(new ConnectionTuneOkBody(_tuneOkChannelMax,
                                                                      _tuneOkFrameMax,
                                                                      _tuneOkHeartbeat));
    }

    public ConnectionInteraction openVirtualHost(String virtualHost)
    {
        _openVirtualHost = virtualHost;
        return this;
    }

    public Interaction open() throws Exception
    {
        return _interaction.sendPerformative(new ConnectionOpenBody(AMQShortString.valueOf(_openVirtualHost),
                                                                    null,
                                                                    false));
    }
}
