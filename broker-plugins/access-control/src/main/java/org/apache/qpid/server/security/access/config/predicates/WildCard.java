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
package org.apache.qpid.server.security.access.config.predicates;

import java.util.Objects;

import javax.security.auth.Subject;

import org.apache.qpid.server.security.access.config.LegacyOperation;
import org.apache.qpid.server.security.access.config.ObjectProperties;
import org.apache.qpid.server.security.access.config.Property;

final class WildCard extends AbstractPredicate
{
    private final String _prefix;

    private final Property _property;

    static RulePredicate newInstance(Property property, String prefix)
    {
        return prefix == null ? RulePredicate.alwaysMatch() : new WildCard(property, prefix);
    }

    private WildCard(Property property, String prefix)
    {
        super();
        _property = Objects.requireNonNull(property);
        _prefix = Objects.requireNonNull(prefix);
    }

    private WildCard(Property property, String prefix, RulePredicate subPredicate)
    {
        super(subPredicate);
        _property = Objects.requireNonNull(property);
        _prefix = Objects.requireNonNull(prefix);
    }

    private WildCard(WildCard predicate, RulePredicate subPredicate)
    {
        this(predicate._property, predicate._prefix, subPredicate);
    }

    @Override
    public boolean matches(LegacyOperation operation, ObjectProperties objectProperties, Subject subject)
    {
        final Object value = objectProperties.get(_property);
        return (value instanceof String) &&
                ((String) value).startsWith(_prefix) &&
                _subPredicate.matches(operation, objectProperties, subject);
    }

    @Override
    RulePredicate copy(RulePredicate subPredicate)
    {
        return new WildCard(this, subPredicate);
    }
}
