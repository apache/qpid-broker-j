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
package org.apache.qpid.server.security.access.config;

import java.util.Set;

import javax.security.auth.Subject;

import org.apache.qpid.server.security.Result;

/**
 * Checks the rule collection based on a subject, operation and objectType.
 * <p>
 * Checks only enabled rules with identity equal to all, owner, the same, or a group with identity as a member,
 * and operation is either all or the same operation,
 * and object type is either all or the same object.
 * </p>
 */
@FunctionalInterface
public interface RuleInspector
{
    /**
     * Check the authorisation granted to a particular identity for an operation on an object type with
     * specific properties.
     * <p>
     * Looks up the entire rule set, which may be cached, for the user and operation and goes through the rules
     * in order to find the first one that matches. Either defers if there are no rules, returns the result of
     * the first match found, or denies access if there are no matching rules. Normally, it would be expected
     * to have a default deny or allow rule at the end of an access configuration however.
     * </p>
     */
    Result check(Subject subject,
                 LegacyOperation operation,
                 ObjectType objectType,
                 ObjectProperties properties);

    interface RuleInspectorFactory
    {
        RuleInspector newInspector(Set<String> relevantPrincipals);

        Set<String> allRuleIdentities();

        default boolean isConstant()
        {
            return allRuleIdentities().isEmpty();
        }
    }
}
