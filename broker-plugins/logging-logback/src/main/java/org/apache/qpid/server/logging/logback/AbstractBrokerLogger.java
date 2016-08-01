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
 * under the License.i
 *
 */
package org.apache.qpid.server.logging.logback;

import java.util.Collection;
import java.util.Map;

import org.apache.qpid.server.logging.LogInclusionRule;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerLogInclusionRule;
import org.apache.qpid.server.model.BrokerLogger;
import org.apache.qpid.server.model.ManagedAttributeField;

public abstract class AbstractBrokerLogger<X extends AbstractBrokerLogger<X>> extends AbstractLogger<X>
        implements BrokerLogger<X>
{
    @ManagedAttributeField
    private boolean _virtualHostLogEventExcluded;

    protected AbstractBrokerLogger(Map<String, Object> attributes, Broker<?> broker)
    {
        super(attributes, broker);
    }

    @Override
    protected void onResolve()
    {
        super.onResolve();
        addLogInclusionRule(new VirtualHostLogEventExcludingFilter(this));
    }

    @Override
    protected Collection<? extends LogInclusionRule> getLogInclusionRules()
    {
        return getChildren(BrokerLogInclusionRule.class);
    }

    @Override
    public boolean isVirtualHostLogEventExcluded()
    {
        return _virtualHostLogEventExcluded;
    }


}
