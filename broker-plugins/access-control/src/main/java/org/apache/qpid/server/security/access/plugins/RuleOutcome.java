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
package org.apache.qpid.server.security.access.plugins;

import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.logging.messages.AccessControlMessages;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.security.access.config.LegacyOperation;
import org.apache.qpid.server.security.access.config.ObjectProperties;
import org.apache.qpid.server.security.access.config.ObjectType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An enumeration of all possible outcomes that can be applied to an access control v2 rule.
 */
public enum RuleOutcome
{
    ALLOW(Result.ALLOWED),
    ALLOW_LOG(Result.ALLOWED)
            {
                @Override
                public Result logResult(EventLoggerProvider logger, LegacyOperation operation, ObjectType objectType, ObjectProperties objectProperties)
                {
                    LOGGER.debug(ACTION_MATCHES_RESULT, this);
                    logger.getEventLogger().message(AccessControlMessages.ALLOWED(
                            operation.toString(),
                            objectType.toString(),
                            objectProperties.toString()));
                    return _result;
                }
            },
    DENY(Result.DENIED),
    DENY_LOG(Result.DENIED)
            {
                @Override
                public Result logResult(EventLoggerProvider logger, LegacyOperation operation, ObjectType objectType, ObjectProperties objectProperties)
                {
                    LOGGER.debug(ACTION_MATCHES_RESULT, this);
                    logger.getEventLogger().message(AccessControlMessages.DENIED(
                            operation.toString(),
                            objectType.toString(),
                            objectProperties.toString()));
                    return _result;
                }
            };

    private static final Logger LOGGER = LoggerFactory.getLogger(RuleOutcome.class);

    private static final String ACTION_MATCHES_RESULT = "Action matches. Result: {}";

    final Result _result;

    RuleOutcome(final Result result)
    {
        _result = result;
    }

    public Result logResult(EventLoggerProvider logger, LegacyOperation operation, ObjectType objectType, ObjectProperties objectProperties)
    {
        LOGGER.debug(ACTION_MATCHES_RESULT, this);
        return _result;
    }
}
