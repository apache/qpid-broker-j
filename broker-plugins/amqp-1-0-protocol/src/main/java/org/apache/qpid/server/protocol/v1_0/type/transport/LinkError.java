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


package org.apache.qpid.server.protocol.v1_0.type.transport;


import org.apache.qpid.server.protocol.v1_0.type.ErrorCondition;
import org.apache.qpid.server.protocol.v1_0.type.RestrictedType;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;

public class LinkError implements ErrorCondition, RestrictedType<Symbol>
{
    public static final LinkError DETACH_FORCED = new LinkError(Symbol.valueOf("amqp:link:detach-forced"));
    public static final LinkError TRANSFER_LIMIT_EXCEEDED =
            new LinkError(Symbol.valueOf("amqp:link:transfer-limit-exceeded"));
    public static final LinkError MESSAGE_SIZE_EXCEEDED =
            new LinkError(Symbol.valueOf("amqp:link:message-size-exceeded"));
    public static final LinkError REDIRECT = new LinkError(Symbol.valueOf("amqp:link:redirect"));
    public static final LinkError STOLEN = new LinkError(Symbol.valueOf("amqp:link:stolen"));

    private final Symbol _val;


    private LinkError(Symbol val)
    {
        _val = val;
    }

    public static LinkError valueOf(Object obj)
    {
        if (obj instanceof Symbol)
        {
            Symbol val = (Symbol) obj;

            if (DETACH_FORCED._val.equals(val))
            {
                return DETACH_FORCED;
            }

            if (TRANSFER_LIMIT_EXCEEDED._val.equals(val))
            {
                return TRANSFER_LIMIT_EXCEEDED;
            }

            if (MESSAGE_SIZE_EXCEEDED._val.equals(val))
            {
                return MESSAGE_SIZE_EXCEEDED;
            }

            if (REDIRECT._val.equals(val))
            {
                return REDIRECT;
            }

            if (STOLEN._val.equals(val))
            {
                return STOLEN;
            }
        }

        final String message = String.format("Cannot convert '%s' into 'link-error'", obj);
        throw new IllegalArgumentException(message);
    }

    @Override
    public Symbol getValue()
    {
        return _val;
    }

    @Override
    public String toString()
    {

        if (this == DETACH_FORCED)
        {
            return "detach-forced";
        }

        if (this == TRANSFER_LIMIT_EXCEEDED)
        {
            return "transfer-limit-exceeded";
        }

        if (this == MESSAGE_SIZE_EXCEEDED)
        {
            return "message-size-exceeded";
        }

        if (this == REDIRECT)
        {
            return "redirect";
        }

        if (this == STOLEN)
        {
            return "stolen";
        }

        else
        {
            return String.valueOf(_val);
        }
    }
}
