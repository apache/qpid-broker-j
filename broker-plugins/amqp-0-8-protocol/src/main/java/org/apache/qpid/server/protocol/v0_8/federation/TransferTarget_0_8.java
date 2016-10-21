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
package org.apache.qpid.server.protocol.v0_8.federation;

import java.util.Collection;
import java.util.Collections;

import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.transfer.TransferQueueEntry;
import org.apache.qpid.server.transfer.TransferTarget;

public class TransferTarget_0_8 implements TransferTarget
{
    private final TransferSession_0_8 _session;
    private final Collection<String> _globalDomains;

    public TransferTarget_0_8(final TransferSession_0_8 transferSession_0_8,
                              final Collection<String> remoteHostGlobalDomains)
    {
        _session = transferSession_0_8;
        _globalDomains = remoteHostGlobalDomains;
    }

    @Override
    public void notifyWork()
    {
        _session.notifyWork();
    }

    @Override
    public Collection<String> getGlobalAddressDomains()
    {
        return Collections.unmodifiableCollection(_globalDomains);
    }

    @Override
    public void send(final TransferQueueEntry entry)
    {
        _session.transfer(entry);
    }

    @Override
    public void restoreCredit(final ServerMessage message)
    {

    }

    @Override
    public boolean wouldSuspend(final TransferQueueEntry entry)
    {
        return _session.isSuspended();
    }

    @Override
    public boolean isSuspended()
    {
        return _session.isSuspended();
    }
}
