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
import org.apache.qpid.server.security.access.config.RulePredicate;

final class AnyValue implements RulePredicate
{
    private final Property _property;

    static RulePredicate newInstance(Property property)
    {
        return new AnyValue(property);
    }

    private AnyValue(Property property)
    {
        super();
        _property = Objects.requireNonNull(property);
    }

    @Override
    public boolean matches(LegacyOperation operation, ObjectProperties objectProperties, Subject subject)
    {
        return objectProperties.get(_property) != null;
    }
}
