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

import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelCloseOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelFlowBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenBody;

public class ChannelInteraction
{
    private Interaction _interaction;

    public ChannelInteraction(final Interaction interaction)
    {
        _interaction = interaction;
    }

    public Interaction open() throws Exception
    {
        return _interaction.sendPerformative(new ChannelOpenBody());
    }

    public Interaction close() throws Exception
    {
        return _interaction.sendPerformative(new ChannelCloseBody(200, AMQShortString.valueOf(""), 0, 0));
    }

    public Interaction closeOk() throws Exception
    {
        return _interaction.sendPerformative(ChannelCloseOkBody.INSTANCE);
    }

    public Interaction flow(final boolean active) throws Exception
    {
        return _interaction.sendPerformative(new ChannelFlowBody(active));
    }
}
