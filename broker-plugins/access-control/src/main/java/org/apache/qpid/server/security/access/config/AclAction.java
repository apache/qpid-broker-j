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
 */
package org.apache.qpid.server.security.access.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.server.security.access.firewall.FirewallRule;

public class AclAction
{
    private Action _action;
    private FirewallRule _firewallRule;

    public AclAction(LegacyOperation operation, ObjectType object, AclRulePredicates predicates)
    {
        _action = new Action(operation, object, predicates.getObjectProperties());
        _firewallRule = predicates.getFirewallRule();
    }

    public AclAction(LegacyOperation operation)
    {
        _action = new Action(operation);
    }

    public AclAction(LegacyOperation operation, ObjectType object, ObjectProperties properties)
    {
        _action = new Action(operation, object, properties);
    }

    public FirewallRule getFirewallRule()
    {
        return _firewallRule;
    }

    public Action getAction()
    {
        return _action;
    }

    public boolean isAllowed()
    {
        return _action.isSupported();
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

        final AclAction aclAction = (AclAction) o;

        if (getAction() != null ? !getAction().equals(aclAction.getAction()) : aclAction.getAction() != null)
        {
            return false;
        }
        return !(getFirewallRule() != null
                ? !getFirewallRule().equals(aclAction.getFirewallRule())
                : aclAction.getFirewallRule() != null);

    }

    @Override
    public int hashCode()
    {
        int result = getAction() != null ? getAction().hashCode() : 0;
        result = 31 * result + (getFirewallRule() != null ? getFirewallRule().hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "AclAction[" +
               "action=" + _action +
               ", firewallRule=" + _firewallRule +
               ']';
    }

    public Map<ObjectProperties.Property, String> getAttributes()
    {
        final ObjectProperties properties = _action.getProperties();
        final Map<ObjectProperties.Property, String> attributes = new HashMap<>(properties.asPropertyMap());
        final Set<String> attributeNames = properties.getAttributeNames();
        if (attributeNames != null && !attributeNames.isEmpty())
        {
            attributes.put(ObjectProperties.Property.ATTRIBUTES, String.join(",", attributeNames));
        }
        final FirewallRule firewallRule = getFirewallRule();
        if (firewallRule != null)
        {
            attributes.put(firewallRule.getPropertyName(), firewallRule.getPropertyValue());
        }
        return attributes;
    }
}
