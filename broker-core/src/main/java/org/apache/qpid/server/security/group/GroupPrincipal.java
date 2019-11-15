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
package org.apache.qpid.server.security.group;

import java.security.Principal;
import java.util.Enumeration;
import java.util.Objects;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.security.QpidPrincipal;

/**
 * Immutable representation of a user group.  In Qpid, groups do <b>not</b> know
 * about their membership, and therefore the {@link #addMember(Principal)}
 * methods etc throw {@link UnsupportedOperationException}.
 *
 */
public class GroupPrincipal implements QpidPrincipal
{
    private static final long serialVersionUID = 1L;

    /** Name of the group */
    private final String _groupName;
    private final ConfiguredObject<?> _origin;
    private static final String msgException = "Not supported";

    public GroupPrincipal(final String groupName, final ConfiguredObject<?> origin)
    {
        if (groupName == null)
        {
            throw new IllegalArgumentException("group name cannot be null");
        }
        _groupName = groupName;
        _origin = origin;
    }

    @Override
    public String getName()
    {
        return _groupName;
    }

    public boolean addMember(Principal user)
    {
        throw new UnsupportedOperationException(msgException);
    }

    public boolean removeMember(Principal user)
    {
        throw new UnsupportedOperationException(msgException);
    }

    public boolean isMember(Principal member)
    {
        throw new UnsupportedOperationException(msgException);
    }

    public Enumeration<? extends Principal> members()
    {
        throw new UnsupportedOperationException(msgException);
    }

    @Override
    public ConfiguredObject<?> getOrigin()
    {
        return _origin;
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

        final GroupPrincipal that = (GroupPrincipal) o;

        if (!_groupName.equals(that._groupName))
        {
            return false;
        }
        return _origin != null ? _origin.equals(that._origin) : that._origin == null;
    }

    @Override
    public int hashCode()
    {
        int result = _groupName.hashCode();
        result = 31 * result + (_origin != null ? _origin.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "GroupPrincipal{" + _groupName + "}";
    }
}
