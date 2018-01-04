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
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeBoundBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeclareBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeleteBody;

public class ExchangeInteraction
{
    private Interaction _interaction;
    private String _declareExchange = "amq.direct";
    private String _declareType = "direct";
    private boolean _declarePassive = false;
    private boolean _declareDurable = false;
    private boolean _declareAutoDelete = false;
    private boolean _declareNoWait = false;
    private Map<String, Object> _declareArguments = new HashMap<>();

    private String _boundQueue;
    private String _boundRoutingKey;
    private String _boundExchange;

    private String _deleteExchange;
    private boolean _deleteIfUnused = false;
    private boolean _deleteNoWait = false;

    public ExchangeInteraction(final Interaction interaction)
    {
        _interaction = interaction;
    }

    public ExchangeInteraction declareName(final String name)
    {
        _declareExchange = name;
        return this;
    }

    public ExchangeInteraction declarePassive(final boolean passive)
    {
        _declarePassive = passive;
        return this;
    }

    public ExchangeInteraction declareDurable(final boolean durable)
    {
        _declareDurable = durable;
        return this;
    }

    public ExchangeInteraction declareType(final String type)
    {
        _declareType = type;
        return this;
    }

    public ExchangeInteraction declareAutoDelete(final boolean autoDelete)
    {
        _declareAutoDelete = autoDelete;
        return this;
    }

    public ExchangeInteraction declareNoWait(final boolean noWait)
    {
        _declareNoWait = noWait;
        return this;
    }

    public ExchangeInteraction declareArguments(final Map<String,Object> args)
    {
        _declareArguments = args == null ? Collections.emptyMap() : new HashMap<>(args);
        return this;
    }

    public Interaction declare() throws Exception
    {
        return _interaction.sendPerformative(new ExchangeDeclareBody(0,
                                                                     AMQShortString.valueOf(_declareExchange),
                                                                     AMQShortString.valueOf(_declareType),
                                                                     _declarePassive,
                                                                     _declareDurable,
                                                                     _declareAutoDelete,
                                                                     false,
                                                                     _declareNoWait,
                                                                     FieldTable.convertToFieldTable(_declareArguments)));
    }

    public ExchangeInteraction boundExchangeName(final String name)
    {
        _boundExchange = name;
        return this;
    }

    public ExchangeInteraction boundQueue(final String name)
    {
        _boundQueue = name;
        return this;
    }

    public ExchangeInteraction boundRoutingKey(final String routingKey)
    {
        _boundRoutingKey = routingKey;
        return this;
    }

    public Interaction bound() throws Exception
    {
        return _interaction.sendPerformative(new ExchangeBoundBody(AMQShortString.valueOf(_boundExchange),
                                                                   AMQShortString.valueOf(_boundRoutingKey),
                                                                   AMQShortString.valueOf(_boundQueue)));
    }

    public ExchangeInteraction deleteExchangeName(final String name)
    {
        _deleteExchange = name;
        return this;
    }

    public ExchangeInteraction deleteIfUnused(final boolean deleteIfUnused)
    {
        _deleteIfUnused = deleteIfUnused;
        return this;
    }

    public Interaction delete() throws Exception
    {
        return _interaction.sendPerformative(new ExchangeDeleteBody(0,
                                                                    AMQShortString.valueOf(_deleteExchange),
                                                                    _deleteIfUnused,
                                                                    _deleteNoWait));
    }
}
