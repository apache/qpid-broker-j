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
package org.apache.qpid.server;

import java.io.Serializable;
import java.security.Principal;

import org.apache.qpid.server.model.Broker;

public class BrokerPrincipal implements Principal, Serializable
{
    private static final long serialVersionUID = 1L;

    private final Broker<?> _broker;
    private final String _name;

    public BrokerPrincipal(Broker<?> broker)
    {
        _broker = broker;
        _name = "broker:" + broker.getName() + "-" + broker.getId();
    }

    @Override
    public String getName()
    {
        return _name;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        BrokerPrincipal that = (BrokerPrincipal) o;
        return _broker.equals(that._broker);
    }

    @Override
    public int hashCode()
    {
        return _broker.hashCode();
    }
}
