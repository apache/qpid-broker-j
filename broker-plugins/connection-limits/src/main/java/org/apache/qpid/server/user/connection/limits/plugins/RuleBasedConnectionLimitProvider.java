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
package org.apache.qpid.server.user.connection.limits.plugins;

import java.util.Collections;
import java.util.List;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Content;
import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedOperation;
import org.apache.qpid.server.model.Param;

@ManagedObject(category = false)
public interface RuleBasedConnectionLimitProvider<X extends ConfiguredObject<X>> extends ConfiguredObject<X>
{
    @ManagedAttribute(mandatory = true, defaultValue = "[ ]", description = "the list of the connection limits")
    default List<ConnectionLimitRule> getRules()
    {
        return Collections.emptyList();
    }

    @ManagedAttribute
    Long getDefaultFrequencyPeriod();

    @ManagedOperation(nonModifying = true,
            description = "Extract the connection limit rules",
            changesConfiguredObjectState = false)
    Content extractRules();

    @ManagedOperation(description = "Load connection limit rules from a file", changesConfiguredObjectState = true)
    void loadFromFile(@Param(name = "path", mandatory = true) String path);

    @ManagedOperation(description = "Clear all connection limits", changesConfiguredObjectState = true)
    void clearRules();

    @ManagedOperation(nonModifying = true,
            description = "Reset the counters of connections",
            changesConfiguredObjectState = false)
    void resetCounters();
}
