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

import org.apache.qpid.server.protocol.v0_10.transport.ExchangeBind;
import org.apache.qpid.server.protocol.v0_10.transport.ExchangeBound;
import org.apache.qpid.server.protocol.v0_10.transport.ExchangeDeclare;
import org.apache.qpid.server.protocol.v0_10.transport.ExchangeDelete;
import org.apache.qpid.server.protocol.v0_10.transport.ExchangeQuery;
import org.apache.qpid.server.protocol.v0_10.transport.ExchangeUnbind;

public class ExchangeInteraction
{
    private final Interaction _interaction;
    private final ExchangeDeclare _declare;
    private final ExchangeDelete _delete;
    private final ExchangeQuery _query;
    private final ExchangeBind _bind;
    private final ExchangeBound _bound;
    private final ExchangeUnbind _unbind;

    public ExchangeInteraction(final Interaction interaction)
    {
        _interaction = interaction;
        _declare = new ExchangeDeclare();
        _delete = new ExchangeDelete();
        _query = new ExchangeQuery();
        _bind = new ExchangeBind();
        _bound = new ExchangeBound();
        _unbind = new ExchangeUnbind();
    }

    public ExchangeInteraction declareExchange(final String exchange)
    {
        _declare.setExchange(exchange);
        return this;
    }

    public ExchangeInteraction declareId(final int id)
    {
        _declare.setId(id);
        return this;
    }
    public ExchangeInteraction declareType(final String type)
    {
        _declare.setType(type);
        return this;
    }

    public ExchangeInteraction declarePassive(final boolean passive)
    {
        _declare.setPassive(passive);
        return this;
    }

    public ExchangeInteraction declareDurable(final boolean durable)
    {
        _declare.setDurable(durable);
        return this;
    }

    public ExchangeInteraction declareAlternateExchange(final String alternateExchange)
    {
        _declare.setAlternateExchange(alternateExchange);
        return this;
    }

    public ExchangeInteraction declareAutoDelete(final boolean autoDelete)
    {
        _declare.setAutoDelete(autoDelete);
        return this;
    }

    public Interaction declare() throws Exception
    {
        return _interaction.sendPerformative(_declare);
    }

    public ExchangeInteraction deleteExchange(final String exchangeName)
    {
        _delete.setExchange(exchangeName);
        return this;
    }

    public ExchangeInteraction deleteId(final int id)
    {
        _delete.setId(id);
        return this;
    }

    public ExchangeInteraction deleteIfUnused(final boolean ifUnused)
    {
        _delete.ifUnused(ifUnused);
        return this;
    }

    public Interaction delete() throws Exception
    {
        return _interaction.sendPerformative(_delete);
    }

    public ExchangeInteraction queryExchange(final String name)
    {
        _query.setName(name);
        return this;
    }

    public ExchangeInteraction queryId(final int id)
    {
        _query.setId(id);
        return this;
    }

    public Interaction query() throws Exception
    {
        return _interaction.sendPerformative(_query);
    }

    public ExchangeInteraction bindId(final int id)
    {
        _bind.setId(id);
        return this;
    }

    public ExchangeInteraction bindQueue(final String queue)
    {
        _bind.setQueue(queue);
        return this;
    }

    public ExchangeInteraction bindExchange(final String exchange)
    {
        _bind.setExchange(exchange);
        return this;
    }

    public ExchangeInteraction bindBindingKey(final String bindingKey)
    {
        _bind.setBindingKey(bindingKey);
        return this;
    }

    public ExchangeInteraction bindArguments(final Map<String, Object> arguments)
    {
        _bind.setArguments(arguments);
        return this;
    }

    public Interaction bind() throws Exception
    {
        return _interaction.sendPerformative(_bind);
    }

    public ExchangeInteraction boundId(final int id)
    {
        _bound.setId(id);
        return this;
    }

    public ExchangeInteraction boundQueue(final String queue)
    {
        _bound.setQueue(queue);
        return this;
    }

    public ExchangeInteraction boundExchange(final String exchange)
    {
        _bound.setExchange(exchange);
        return this;
    }

    public ExchangeInteraction boundBindingKey(final String bindingKey)
    {
        _bound.setBindingKey(bindingKey);
        return this;
    }

    public ExchangeInteraction boundArguments(final Map<String, Object> arguments)
    {
        _bound.setArguments(arguments);
        return this;
    }

    public Interaction bound() throws Exception
    {
        return _interaction.sendPerformative(_bound);
    }


    public ExchangeInteraction unbindId(final int id)
    {
        _unbind.setId(id);
        return this;
    }

    public ExchangeInteraction unbindQueue(final String queue)
    {
        _unbind.setQueue(queue);
        return this;
    }

    public ExchangeInteraction unbindExchange(final String exchange)
    {
        _unbind.setExchange(exchange);
        return this;
    }

    public ExchangeInteraction unbindBindingKey(final String bindingKey)
    {
        _unbind.setBindingKey(bindingKey);
        return this;
    }

    public Interaction unbind() throws Exception
    {
        return _interaction.sendPerformative(_unbind);
    }

    public ExchangeInteraction declareArguments(final Map<String, Object> arguments)
    {
        _declare.setArguments(arguments);
        return this;
    }
}
