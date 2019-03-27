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
package org.apache.qpid.server.message;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.qpid.server.plugin.Pluggable;
import org.apache.qpid.server.plugin.QpidServiceLoader;
import org.apache.qpid.server.store.MessageStore;

public interface ServerMessageMutatorFactory<T extends ServerMessage> extends Pluggable
{
    ServerMessageMutator<T> create(T serverMessage, MessageStore messageStore);

    static <T extends ServerMessage> ServerMessageMutator<T> createMutator(T serverMessage, MessageStore messageStore)
    {
        final ServerMessageMutatorFactory<T> factory =
                ServerMessageMutatorFactoryRegistry.get(serverMessage.getClass().getName());
        if (factory == null)
        {
            throw new IllegalStateException(String.format("Cannot find server message mutator for message class '%s'",
                                                          serverMessage.getClass().getName()));
        }
        return factory.create(serverMessage, messageStore);
    }

    class ServerMessageMutatorFactoryRegistry
    {
        private static Map<String, ServerMessageMutatorFactory> MUTATOR_FACTORIES =
                StreamSupport.stream(new QpidServiceLoader().instancesOf(ServerMessageMutatorFactory.class)
                                                            .spliterator(), false).collect(
                        Collectors.toMap(Pluggable::getType, i -> i));

        private static ServerMessageMutatorFactory get(String type)
        {
            return MUTATOR_FACTORIES.get(type);
        }
    }
}
