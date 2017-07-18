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
package org.apache.qpid.server.protocol.v1_0;

import static org.apache.qpid.server.model.LifetimePolicy.PERMANENT;

import java.util.ArrayList;
import java.util.List;

import org.apache.qpid.server.message.MessageSource;
import org.apache.qpid.server.model.LifetimePolicy;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.protocol.v1_0.type.Outcome;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;

public class StandardSendingDestination implements SendingDestination
{
    private static final Accepted ACCEPTED = new Accepted();
    private static final Outcome[] OUTCOMES = new Outcome[] { ACCEPTED };


    private final MessageSource _messageSource;
    private final Symbol[] _capabilities;

    public StandardSendingDestination(MessageSource messageSource)
    {
        _messageSource = messageSource;
        List<Symbol> capabilities = new ArrayList<>();
        if (_messageSource instanceof Queue)
        {
            LifetimePolicy queueLifetimePolicy = ((Queue<?>) _messageSource).getLifetimePolicy();
            if (PERMANENT == queueLifetimePolicy)
            {
                capabilities.add(Symbol.getSymbol("queue"));
            }
            else
            {
                capabilities.add(Symbol.getSymbol("temporary-queue"));
            }
        }
        _capabilities = capabilities.toArray(new Symbol[capabilities.size()]);
    }

    public Outcome[] getOutcomes()
    {
        return OUTCOMES;
    }

    @Override
    public MessageSource getMessageSource()
    {
        return _messageSource;
    }

    @Override
    public Symbol[] getCapabilities()
    {
        return _capabilities;
    }
}
