
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


import org.apache.qpid.server.protocol.v1_0.type.RestrictedType;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;

public class TerminusExpiryPolicy implements RestrictedType<Symbol>
{
    private final Symbol _val;

    public static final TerminusExpiryPolicy LINK_DETACH = new TerminusExpiryPolicy(Symbol.valueOf("link-detach"));

    public static final TerminusExpiryPolicy SESSION_END = new TerminusExpiryPolicy(Symbol.valueOf("session-end"));

    public static final TerminusExpiryPolicy CONNECTION_CLOSE =
            new TerminusExpiryPolicy(Symbol.valueOf("connection-close"));

    public static final TerminusExpiryPolicy NEVER = new TerminusExpiryPolicy(Symbol.valueOf("never"));

    private TerminusExpiryPolicy(Symbol val)
    {
        _val = val;
    }

    @Override
    public Symbol getValue()
    {
        return _val;
    }

    @Override
    public String toString()
    {

        if (this == LINK_DETACH)
        {
            return "link-detach";
        }

        if (this == SESSION_END)
        {
            return "session-end";
        }

        if (this == CONNECTION_CLOSE)
        {
            return "connection-close";
        }

        if (this == NEVER)
        {
            return "never";
        }

        else
        {
            return String.valueOf(_val);
        }
    }

    public static TerminusExpiryPolicy valueOf(Object obj)
    {
        if (obj instanceof Symbol)
        {
            Symbol val = (Symbol) obj;

            if (LINK_DETACH._val.equals(val))
            {
                return LINK_DETACH;
            }

            if (SESSION_END._val.equals(val))
            {
                return SESSION_END;
            }

            if (CONNECTION_CLOSE._val.equals(val))
            {
                return CONNECTION_CLOSE;
            }

            if (NEVER._val.equals(val))
            {
                return NEVER;
            }
        }

        final String message = String.format("Cannot convert '%s' into 'terminus-expiry-policy'", obj);
        throw new IllegalArgumentException(message);
    }
}
