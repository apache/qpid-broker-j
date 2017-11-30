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

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeclareBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeleteBody;

public class QueueInteraction
{
    private Interaction _interaction;
    private String _declareName;
    private boolean _declarePassive;
    private boolean _declareDurable;
    private boolean _declareExclusive;
    private boolean _declareAutoDelete;
    private boolean _declareNowait;
    private Map<String, Object> _declareArguments = new HashMap<>();

    private String _deleteName;
    private boolean _deleteIfUnused;
    private boolean _deleteIfEmpty;
    private boolean _deleteNowait;

    public QueueInteraction(final Interaction interaction)
    {
        _interaction = interaction;
    }

    public QueueInteraction declareName(String name)
    {
        _declareName = name;
        return this;
    }

    public QueueInteraction declarePassive(final boolean declarePassive)
    {
        _declarePassive = declarePassive;
        return this;
    }

    public QueueInteraction declareDurable(final boolean declareDurable)
    {
        _declareDurable = declareDurable;
        return this;
    }

    public QueueInteraction declareAutoDelete(final boolean autoDelete)
    {
        _declareAutoDelete = autoDelete;
        return this;
    }

    public QueueInteraction declareExclusive(final boolean exclusive)
    {
        _declareExclusive = exclusive;
        return this;
    }

    public Interaction declare() throws Exception
    {
        return _interaction.sendPerformative(new QueueDeclareBody(0,
                                                                  AMQShortString.valueOf(_declareName),
                                                                  _declarePassive,
                                                                  _declareDurable,
                                                                  _declareExclusive,
                                                                  _declareAutoDelete,
                                                                  _declareNowait,
                                                                  FieldTable.convertToFieldTable(_declareArguments)));
    }

    public QueueInteraction deleteName(final String name)
    {
        _deleteName = name;
        return this;
    }

    public QueueInteraction deleteIfUnused(final boolean deleteIfUnused)
    {
        _deleteIfUnused = deleteIfUnused;
        return this;
    }

    public Interaction delete() throws Exception
    {
        return _interaction.sendPerformative(new QueueDeleteBody(0,
                                                                 AMQShortString.valueOf(_deleteName),
                                                                 _deleteIfUnused,
                                                                 _deleteIfEmpty,
                                                                 _deleteNowait));
    }
}
