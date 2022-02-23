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
package org.apache.qpid.server.security.access.config;

import java.util.Collection;
import java.util.List;

import javax.security.auth.Subject;

import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.security.Result;

/**
 * Models the rule configuration for the access control plugin.
 * RuleSet checks the rule list based on a subject, operation and objectType.
 * <p>
 * Allows only enabled rules with identity equal to all, owner, the same, or a group with identity as a member,
 * and operation is either all or the same operation,
 * and object type is either all or the same object.
 * </p>
 */
public interface RuleSet extends EventLoggerProvider, List<Rule>, RuleInspector
{
    @Override
    Result check(Subject subject,
                 LegacyOperation operation,
                 ObjectType objectType,
                 ObjectProperties properties);

    default Result getDefault()
    {
        return Result.DENIED;
    }

    static Builder newBuilder(EventLoggerProvider eventLogger)
    {
        return new RuleSetBuilder(eventLogger);
    }

    static RuleSet newInstance(EventLoggerProvider eventLogger, Collection<? extends Rule> rules, Result defaultResult)
    {
        return newBuilder(eventLogger).addAllRules(rules).setDefaultResult(defaultResult).build();
    }

    interface Builder
    {
        Builder setDefaultResult(Result result);

        Builder addAllRules(Collection<? extends Rule> rules);

        RuleSet build();
    }
}
