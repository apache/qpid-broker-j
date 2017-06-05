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

package org.apache.qpid.server.protocol.v1_0;

import org.apache.qpid.server.protocol.LinkModel;
import org.apache.qpid.server.protocol.v1_0.type.BaseSource;
import org.apache.qpid.server.protocol.v1_0.type.BaseTarget;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.util.Action;

public class LinkDefinitionImpl<S extends BaseSource, T extends BaseTarget> implements LinkDefinition<S, T>
{
    private final String _remoteContainerId;
    private final String _name;
    private final Role _role;
    private final S _source;
    private final T _target;

    public LinkDefinitionImpl(final String remoteContainerId,
                              final String name,
                              final Role role,
                              final S source,
                              final T target)
    {
        _remoteContainerId = remoteContainerId;
        _name = name;
        _role = role;
        _source = source;
        _target = target;
    }

    @Override
    public String getRemoteContainerId()
    {
        return _remoteContainerId;
    }

    @Override
    public String getName()
    {
        return _name;
    }

    @Override
    public Role getRole()
    {
        return _role;
    }

    @Override
    public S getSource()
    {
        return _source;
    }

    @Override
    public T getTarget()
    {
        return _target;
    }

    @Override
    public void addDeleteTask(final Action<? super LinkModel> task)
    {
        throw new UnsupportedOperationException("");
    }

    @Override
    public void removeDeleteTask(final Action<? super LinkModel> task)
    {
        throw new UnsupportedOperationException("");
    }
}
