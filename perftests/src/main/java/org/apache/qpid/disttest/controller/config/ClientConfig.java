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
package org.apache.qpid.disttest.controller.config;

import java.util.ArrayList;
import java.util.List;

import org.apache.qpid.disttest.controller.CommandForClient;
import org.apache.qpid.disttest.message.Command;

public class ClientConfig
{
    private final List<ConnectionConfig> _connections;
    private final List<MessageProviderConfig> _messageProviders;
    private final String _name;

    public ClientConfig()
    {
        _name = null;
        _connections = List.of();
        _messageProviders = List.of();
    }

    public ClientConfig(String name, ConnectionConfig... connections)
    {
        this(name, List.of(connections), null);
    }

    public ClientConfig(String name, List<ConnectionConfig> connections, List<MessageProviderConfig> messageProviders)
    {
        _name = name;
        _connections = connections;
        if (messageProviders == null)
        {
            _messageProviders = List.of();
        }
        else
        {
            _messageProviders = messageProviders;
        }
    }

    public String getName()
    {
        return _name;
    }

    public List<ConnectionConfig> getConnections()
    {
        return List.copyOf(_connections);
    }

    public List<CommandForClient> createCommands()
    {
        List<CommandForClient> commandsForClient = new ArrayList<>();

        for (MessageProviderConfig messageProvider : _messageProviders)
        {
            Command command = messageProvider.createCommand();
            commandsForClient.add(new CommandForClient(_name, command));
        }
        for (ConnectionConfig connection : _connections)
        {
            List<Command> commands = connection.createCommands();
            for (Command command : commands)
            {
                commandsForClient.add(new CommandForClient(_name, command));
            }
        }
        return commandsForClient;
    }

    public int getTotalNumberOfParticipants()
    {
        int numOfParticipants = 0;
        for (ConnectionConfig connection : _connections)
        {
            numOfParticipants = numOfParticipants + connection.getTotalNumberOfParticipants();
        }
        return numOfParticipants;
    }

    public List<MessageProviderConfig> getMessageProviders()
    {
        return List.copyOf(_messageProviders);
    }


}
