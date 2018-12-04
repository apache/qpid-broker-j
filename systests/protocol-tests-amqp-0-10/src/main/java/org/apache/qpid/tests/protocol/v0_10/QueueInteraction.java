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
package org.apache.qpid.tests.protocol.v0_10;

import java.util.Map;

import org.apache.qpid.server.protocol.v0_10.transport.QueueDeclare;
import org.apache.qpid.server.protocol.v0_10.transport.QueueDelete;
import org.apache.qpid.server.protocol.v0_10.transport.QueuePurge;
import org.apache.qpid.server.protocol.v0_10.transport.QueueQuery;

public class QueueInteraction
{
    private final Interaction _interaction;
    private final QueueDeclare _declare;
    private final QueueDelete _delete;
    private final QueuePurge _purge;
    private final QueueQuery _query;

    public QueueInteraction(final Interaction interaction)
    {
        _interaction = interaction;
        _declare = new QueueDeclare();
        _delete = new QueueDelete();
        _purge = new QueuePurge();
        _query = new QueueQuery();
    }

    public QueueInteraction declareQueue(final String queue)
    {
        _declare.setQueue(queue);
        return this;
    }

    public QueueInteraction declareId(final int id)
    {
        _declare.setId(id);
        return this;
    }
    public QueueInteraction declarePassive(final boolean passive)
    {
        _declare.setPassive(passive);
        return this;
    }

    public QueueInteraction declareDurable(final boolean durable)
    {
        _declare.setDurable(durable);
        return this;
    }

    public QueueInteraction declareAlternateExchange(final String alternateExchange)
    {
        _declare.setAlternateExchange(alternateExchange);
        return this;
    }

    public QueueInteraction declareExclusive(final boolean exclusive)
    {
        _declare.setExclusive(exclusive);
        return this;
    }

    public QueueInteraction declareAutoDelete(final boolean autoDelete)
    {
        _declare.setAutoDelete(autoDelete);
        return this;
    }

    public Interaction declare() throws Exception
    {
        return _interaction.sendPerformative(_declare);
    }

    public QueueInteraction deleteQueue(final String queueName)
    {
        _delete.setQueue(queueName);
        return this;
    }

    public QueueInteraction deleteId(final int id)
    {
        _delete.setId(id);
        return this;
    }

    public QueueInteraction deleteIfUnused(final boolean ifUnused)
    {
        _delete.ifUnused(ifUnused);
        return this;
    }

    public Interaction delete() throws Exception
    {
        return _interaction.sendPerformative(_delete);
    }

    public QueueInteraction purgeQueue(final String queueName)
    {
        _purge.setQueue(queueName);
        return this;
    }

    public QueueInteraction purgeId(final int id)
    {
        _purge.setId(id);
        return this;
    }
    public Interaction purge() throws Exception
    {
        return _interaction.sendPerformative(_purge);
    }

    public QueueInteraction queryQueue(final String queueName)
    {
        _query.setQueue(queueName);
        return this;
    }

    public QueueInteraction queryId(final int id)
    {
        _query.setId(id);
        return this;
    }

    public Interaction query() throws Exception
    {
        return _interaction.sendPerformative(_query);
    }

    public QueueInteraction declareArguments(final Map<String, Object> arguments)
    {
        _declare.setArguments(arguments);
        return this;
    }
}
