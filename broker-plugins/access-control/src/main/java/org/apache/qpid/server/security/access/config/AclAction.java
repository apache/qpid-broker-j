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

import java.util.Map;
import java.util.Objects;

public class AclAction
{
    private Action _action;
    private AclRulePredicates _predicates;

    public AclAction(LegacyOperation operation, ObjectType object, AclRulePredicates predicates)
    {
        _action = new Action(operation, object, predicates.getObjectProperties());
        _predicates = predicates;
    }

    public AclAction(LegacyOperation operation)
    {
        _action = new Action(operation);
    }

    public AclAction(LegacyOperation operation, ObjectType object, ObjectProperties properties)
    {
        _action = new Action(operation, object, properties);
    }

    public DynamicRule getDynamicRule()
    {
        return _predicates == null ? null : _predicates.getDynamicRule();
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
        return Objects.equals(_predicates, aclAction._predicates);

    }

    @Override
    public int hashCode()
    {
        int result = getAction() != null ? getAction().hashCode() : 0;
        result = 31 * result + (_predicates != null ? _predicates.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "AclAction[" +
               "action=" + _action +
               ", predicates=" + _predicates +
               ']';
    }

    public Map<ObjectProperties.Property, String> getAttributes()
    {
        return _predicates.getParsedProperties();
    }

}
