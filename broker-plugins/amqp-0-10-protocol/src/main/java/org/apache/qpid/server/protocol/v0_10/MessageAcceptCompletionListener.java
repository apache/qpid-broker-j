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

package org.apache.qpid.server.protocol.v0_10;

import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.protocol.v0_10.transport.Method;

public class MessageAcceptCompletionListener implements Method.CompletionListener
{
    private final ConsumerTarget_0_10 _sub;
    private final MessageInstance _entry;
    private final ServerSession _session;
    private final MessageInstanceConsumer _consumer;
    private final boolean _restoreCredit;
    private long _messageSize;

    public MessageAcceptCompletionListener(ConsumerTarget_0_10 sub,
                                           final MessageInstanceConsumer consumer,
                                           ServerSession session,
                                           MessageInstance entry,
                                           boolean restoreCredit)
    {
        super();
        _sub = sub;
        _entry = entry;
        _session = session;
        _restoreCredit = restoreCredit;
        _consumer = consumer;
        if(restoreCredit)
        {
            _messageSize = entry.getMessage().getSize();
        }
    }

    @Override
    public void onComplete(Method method)
    {
        if(_restoreCredit)
        {
            _sub.restoreCredit(1, _messageSize);
        }
        _session.acknowledge(_consumer, _sub, _entry);

        _session.removeDispositionListener(method);
    }
}
