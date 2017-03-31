/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.qpid.server.protocol.v1_0;

import org.apache.qpid.server.protocol.v1_0.type.transport.Role;

public class LinkKey
{
    private final String _linkName;
    private final String _remoteContainerId;
    private final Role _role;

    public LinkKey(final String remoteContainerId, final String linkName, final Role role)
    {
        _linkName = linkName;
        _remoteContainerId = remoteContainerId;
        _role = role;
    }

    public LinkKey(final LinkDefinition<?, ?> link)
    {
        this(link.getRemoteContainerId(), link.getName(), link.getRole());
    }

    public String getLinkName()
    {
        return _linkName;
    }

    public String getRemoteContainerId()
    {
        return _remoteContainerId;
    }

    public Role getRole()
    {
        return _role;
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        final LinkKey linkKey = (LinkKey) o;

        if (!_linkName.equals(linkKey._linkName))
        {
            return false;
        }
        if (!_remoteContainerId.equals(linkKey._remoteContainerId))
        {
            return false;
        }
        return _role.equals(linkKey._role);
    }

    @Override
    public int hashCode()
    {
        int result = _linkName.hashCode();
        result = 31 * result + _remoteContainerId.hashCode();
        result = 31 * result + _role.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "LinkKey{" +
               "linkName='" + _linkName + '\'' +
               ", remoteContainerId='" + _remoteContainerId + '\'' +
               ", role=" + _role +
               '}';
    }
}
