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

import java.util.HashSet;
import java.util.Set;

import javax.security.auth.Subject;

import org.apache.qpid.server.security.access.config.LegacyOperation;
import org.apache.qpid.server.security.access.config.ObjectProperties;

final class AttributeNames extends AbstractPredicate
{
    private final Set<String> _attributeNames;

    static RulePredicate newInstance(Set<String> attributeNames)
    {
        return attributeNames.isEmpty() ? RulePredicate.alwaysMatch() : new AttributeNames(attributeNames);
    }

    private AttributeNames(Set<String> attributeNames)
    {
        super();
        _attributeNames = new HashSet<>(attributeNames);
    }

    private AttributeNames(AttributeNames predicate, RulePredicate subPredicate)
    {
        super(subPredicate);
        _attributeNames = predicate._attributeNames;
    }

    @Override
    public boolean matches(LegacyOperation operation, ObjectProperties objectProperties, Subject subject)
    {
        return (operation != LegacyOperation.UPDATE ||
                _attributeNames.containsAll(objectProperties.getAttributeNames())) &&
                _subPredicate.matches(operation, objectProperties, subject);
    }

    @Override
    RulePredicate copy(RulePredicate subPredicate)
    {
        return new AttributeNames(this, subPredicate);
    }
}
