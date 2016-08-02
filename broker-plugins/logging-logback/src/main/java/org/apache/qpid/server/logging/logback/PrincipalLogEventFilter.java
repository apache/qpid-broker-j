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
package org.apache.qpid.server.logging.logback;

import java.security.AccessController;
import java.security.Principal;

import javax.security.auth.Subject;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;


public class PrincipalLogEventFilter extends Filter<ILoggingEvent> implements LogBackLogInclusionRule
{
    private final Principal _principal;

    public PrincipalLogEventFilter(final Principal principal)
    {
        _principal = principal;
    }

    @Override
    public FilterReply decide(ILoggingEvent event)
    {
        Subject subject = Subject.getSubject(AccessController.getContext());
        if (subject != null && subject.getPrincipals().contains(_principal))
        {
            return FilterReply.NEUTRAL;
        }
        return FilterReply.DENY;
    }

    @Override
    public Filter<ILoggingEvent> asFilter()
    {
        return this;
    }

    @Override
    public String getName()
    {
        return "$" + getClass().getName();
    }
}
