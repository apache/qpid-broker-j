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

import java.util.Map;

import org.apache.qpid.server.security.access.plugins.RuleOutcome;

/**
 * An access control v2 rule.
 *
 * A rule consists of {@link RuleOutcome} for a particular identity to perform an {@link Action}. The identity
 * may be either a user or a group.
 */
public class Rule
{
	/** String indicating all identified. */
	public static final String ALL = "all";

	/** String indicating all identified. */
	public static final String OWNER = "owner";

    private final String _identity;
    private final AclAction _action;
    private final RuleOutcome _ruleOutcome;

    public Rule(String identity, AclAction action, RuleOutcome ruleOutcome)
    {
        _identity = identity;
        _action = action;
        _ruleOutcome = ruleOutcome;
    }

    public String getIdentity()
    {
        return _identity;
    }

    public Action getAction()
    {
        return _action.getAction();
    }

    public AclAction getAclAction()
    {
        return _action;
    }

    public RuleOutcome getRuleOutcome()
    {
        return _ruleOutcome;
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

        if (getIdentity() != null ? !getIdentity().equals(rule.getIdentity()) : rule.getIdentity() != null)
        {
            return false;
        }
        if (getAction() != null ? !getAction().equals(rule.getAction()) : rule.getAction() != null)
        {
            return false;
        }
        return getRuleOutcome() == rule.getRuleOutcome();

    }

    @Override
    public int hashCode()
    {
        int result = (getIdentity() != null ? getIdentity().hashCode() : 0);
        result = 31 * result + (getAction() != null ? getAction().hashCode() : 0);
        result = 31 * result + (getRuleOutcome() != null ? getRuleOutcome().hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "Rule[" +
               "identity='" + _identity + '\'' +
               ", action=" + _action +
               ", permission=" + _ruleOutcome +
               ']';
    }

    public Map<ObjectProperties.Property, String> getAttributes()
    {
        return _action.getAttributes();
    }
}
