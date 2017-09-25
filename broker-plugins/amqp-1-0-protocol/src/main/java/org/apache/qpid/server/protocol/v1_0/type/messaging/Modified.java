
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


package org.apache.qpid.server.protocol.v1_0.type.messaging;


import java.util.Map;

import org.apache.qpid.server.protocol.v1_0.CompositeType;
import org.apache.qpid.server.protocol.v1_0.CompositeTypeField;
import org.apache.qpid.server.protocol.v1_0.type.Outcome;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;

@CompositeType( symbolicDescriptor = "amqp:modified:list", numericDescriptor = 0x0000000000000027L)
public class Modified implements Outcome
{
    public static final Symbol MODIFIED_SYMBOL = Symbol.valueOf("amqp:modified:list");

    @CompositeTypeField(index = 0)
    private Boolean _deliveryFailed;

    @CompositeTypeField(index = 1)
    private Boolean _undeliverableHere;

    @CompositeTypeField(index = 2)
    private Map<Symbol, Object> _messageAnnotations;

    public Boolean getDeliveryFailed()
    {
        return _deliveryFailed;
    }

    public void setDeliveryFailed(Boolean deliveryFailed)
    {
        _deliveryFailed = deliveryFailed;
    }

    public Boolean getUndeliverableHere()
    {
        return _undeliverableHere;
    }

    public void setUndeliverableHere(Boolean undeliverableHere)
    {
        _undeliverableHere = undeliverableHere;
    }

    public Map<Symbol, Object> getMessageAnnotations()
    {
        return _messageAnnotations;
    }

    public void setMessageAnnotations(Map<Symbol, Object> messageAnnotations)
    {
        _messageAnnotations = messageAnnotations;
    }

    @Override
    public Symbol getSymbol()
    {
        return MODIFIED_SYMBOL;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("Modified{");
        final int origLength = builder.length();

        if (_deliveryFailed != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("deliveryFailed=").append(_deliveryFailed);
        }

        if (_undeliverableHere != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("undeliverableHere=").append(_undeliverableHere);
        }

        if (_messageAnnotations != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("messageAnnotations=").append(_messageAnnotations);
        }

        builder.append('}');
        return builder.toString();
    }
}
