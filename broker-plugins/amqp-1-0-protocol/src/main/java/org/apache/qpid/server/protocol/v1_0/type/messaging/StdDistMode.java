
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


import org.apache.qpid.server.protocol.v1_0.constants.Symbols;
import org.apache.qpid.server.protocol.v1_0.type.DistributionMode;
import org.apache.qpid.server.protocol.v1_0.type.RestrictedType;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;

public class StdDistMode implements DistributionMode, RestrictedType<Symbol>
{
    private final Symbol _val;

    public static final StdDistMode MOVE = new StdDistMode(Symbols.MOVE);

    public static final StdDistMode COPY = new StdDistMode(Symbols.COPY);

    private StdDistMode(Symbol val)
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
        if (this == MOVE)
        {
            return "move";
        }

        if (this == COPY)
        {
            return "copy";
        }

        else
        {
            return String.valueOf(_val);
        }
    }

    public static StdDistMode valueOf(Object obj)
    {
        if (obj instanceof Symbol)
        {
            Symbol val = (Symbol) obj;

            if (MOVE._val.equals(val))
            {
                return MOVE;
            }

            if (COPY._val.equals(val))
            {
                return COPY;
            }
        }

        final String message = String.format("Cannot convert '%s' into 'std-dist-mode'", obj);
        throw new IllegalArgumentException(message);
    }
}
