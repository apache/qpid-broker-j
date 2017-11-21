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
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeclareBody;

public class ExchangeInteraction
{
    private Interaction _interaction;
    private String _decalreExchange = "amq.direct";
    private String _declareType = "direct";
    private boolean _declarePassive = true;
    private boolean _declareDurable = true;
    private boolean _declareAutoDelete = false;
    private boolean _declareNoWait = false;
    private Map<String, Object> _declareArguments = new HashMap<>();

    public ExchangeInteraction(final Interaction interaction)
    {
        _interaction = interaction;
    }

    public Interaction declare() throws Exception
    {
        return _interaction.sendPerformative(new ExchangeDeclareBody(0,
                                                                     AMQShortString.valueOf(_decalreExchange),
                                                                     AMQShortString.valueOf(_declareType),
                                                                     _declarePassive,
                                                                     _declareDurable,
                                                                     _declareAutoDelete,
                                                                     false,
                                                                     _declareNoWait,
                                                                     FieldTable.convertToFieldTable(_declareArguments)));
    }
}
