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
 *
 *
 */
package org.apache.qpid.server.security.access.config;

import org.apache.qpid.server.security.access.Permission;

/**
 * An access control v2 rule.
 *
 * A rule consists of {@link Permission} for a particular identity to perform an {@link Action}. The identity
 * may be either a user or a group.
 */
public class Rule
{
	/** String indicating all identified. */
	public static final String ALL = "all";

    private Integer _number;
    private String _identity;
    private AclAction _action;
    private Permission _permission;
    private Boolean _enabled = Boolean.TRUE;

    public Rule(Integer number, String identity, AclAction action, Permission permission)
    {
        setNumber(number);
        setIdentity(identity);
        setAction(action);
        setPermission(permission);
    }

    public Rule(String identity, AclAction action, Permission permission)
    {
        this(null, identity, action, permission);
    }

    public boolean isEnabled()
    {
        return _enabled;
    }

    public void setEnabled(boolean enabled)
    {
        _enabled = enabled;
    }

    public void enable()
    {
        _enabled = Boolean.TRUE;
    }

    public void disable()
    {
        _enabled = Boolean.FALSE;
    }

    public Integer getNumber()
    {
        return _number;
    }

    public void setNumber(Integer number)
    {
        _number = number;
    }

    public String getIdentity()
    {
        return _identity;
    }

    public void setIdentity(String identity)
    {
        _identity = identity;
    }

    public Action getAction()
    {
        return _action.getAction();
    }

    public AclAction getAclAction()
    {
        return _action;
    }

    public void setAction(AclAction action)
    {
        _action = action;
    }

    public Permission getPermission()
    {
        return _permission;
    }

    public void setPermission(Permission permission)
    {
        _permission = permission;
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

        final Rule rule = (Rule) o;

        if (getNumber() != null ? !getNumber().equals(rule.getNumber()) : rule.getNumber() != null)
        {
            return false;
        }
        if (getIdentity() != null ? !getIdentity().equals(rule.getIdentity()) : rule.getIdentity() != null)
        {
            return false;
        }
        if (getAction() != null ? !getAction().equals(rule.getAction()) : rule.getAction() != null)
        {
            return false;
        }
        return getPermission() == rule.getPermission();

    }

    @Override
    public int hashCode()
    {
        int result = getNumber() != null ? getNumber().hashCode() : 0;
        result = 31 * result + (getIdentity() != null ? getIdentity().hashCode() : 0);
        result = 31 * result + (getAction() != null ? getAction().hashCode() : 0);
        result = 31 * result + (getPermission() != null ? getPermission().hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "Rule[" +
               "#=" + _number +
               ", identity='" + _identity + '\'' +
               ", action=" + _action +
               ", permission=" + _permission +
               ", enabled=" + _enabled +
               ']';
    }


}
