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

import java.util.Objects;

import javax.security.auth.Subject;

@FunctionalInterface
public interface RulePredicate
{
    boolean matches(LegacyOperation operation, ObjectProperties objectProperties, Subject subject);

    default RulePredicate and(RulePredicate other)
    {
        if (other instanceof Any)
        {
            return this;
        }
        Objects.requireNonNull(other);
        return (operation, objectProperties, subject) ->
                RulePredicate.this.matches(operation, objectProperties, subject)
                && other.matches(operation, objectProperties, subject);
    }

    default RulePredicate or(RulePredicate other)
    {
        if (other instanceof Any)
        {
            return other;
        }
        Objects.requireNonNull(other);
        return (operation, objectProperties, subject) ->
                RulePredicate.this.matches(operation, objectProperties, subject)
                || other.matches(operation, objectProperties, subject);
    }

    static RulePredicate any()
    {
        return Any.INSTANCE;
    }

    final class Any implements RulePredicate
    {
        static final RulePredicate INSTANCE = new Any();

        private Any()
        {
            super();
        }

        @Override
        public boolean matches(LegacyOperation operation, ObjectProperties objectProperties, Subject subject)
        {
            return true;
        }

        @Override
        public RulePredicate and(RulePredicate other)
        {
            return Objects.requireNonNull(other);
        }

        @Override
        public RulePredicate or(RulePredicate other)
        {
            return this;
        }
    }
}
