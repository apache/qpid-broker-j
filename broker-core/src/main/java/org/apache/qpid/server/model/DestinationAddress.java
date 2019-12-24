/*
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

package org.apache.qpid.server.model;

import org.apache.qpid.server.message.MessageDestination;

public class DestinationAddress
{
    private final MessageDestination _messageDestination;
    private final String _routingKey;
    private final String _routingAddress;

    public DestinationAddress(NamedAddressSpace addressSpace, String routingAddress)
    {
        this(addressSpace, routingAddress, false);
    }

    public DestinationAddress(NamedAddressSpace addressSpace, String routingAddress, boolean mayCreate)
    {
        MessageDestination destination = null;
        String routingKey = routingAddress;
        if (routingAddress != null && !routingAddress.trim().equals(""))
        {
            String localRoutingAddress = addressSpace.getLocalAddress(routingAddress);
            if (!localRoutingAddress.contains("/"))
            {
                destination = addressSpace.getAttainedMessageDestination(localRoutingAddress, mayCreate);
                if (destination != null)
                {
                    routingKey = "";
                }
            }
            else if (!localRoutingAddress.startsWith("/"))
            {
                String[] parts = localRoutingAddress.split("/", 2);
                destination = addressSpace.getAttainedMessageDestination(parts[0], mayCreate);
                if (destination instanceof Exchange)
                {
                    routingKey = parts[1];
                }
                else
                {
                    destination = null;
                }
            }
        }
        _routingAddress = routingAddress == null ? "" : routingAddress;
        _messageDestination = destination;
        _routingKey = routingKey == null ? "" : routingKey;
    }

    public MessageDestination getMessageDestination()
    {
        return _messageDestination;
    }

    public String getRoutingKey()
    {
        return _routingKey;
    }

    public String getRoutingAddress()
    {
        return _routingAddress;
    }
}
