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

import java.util.Collection;
import java.util.Map;

import org.apache.qpid.server.logging.LogInclusionRule;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostLogger;
import org.apache.qpid.server.model.VirtualHostLogInclusionRule;

public abstract class AbstractVirtualHostLogger <X extends AbstractVirtualHostLogger<X>> extends AbstractLogger<X>
        implements VirtualHostLogger<X>
{

    private final VirtualHost<?> _virtualHost;

    protected AbstractVirtualHostLogger(Map<String, Object> attributes, VirtualHost<?> virtualHost)
    {
        super(attributes, virtualHost);
        _virtualHost = virtualHost;
    }

    @Override
    protected void onResolve()
    {
        super.onResolve();
        addLogInclusionRule(new PrincipalLogEventFilter(_virtualHost.getPrincipal()));
    }

    @Override
    protected Collection<? extends LogInclusionRule> getLogInclusionRules()
    {
        return getChildren(VirtualHostLogInclusionRule.class);
    }

}
