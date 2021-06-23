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

import java.util.List;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.DerivedAttribute;
import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedOperation;

@ManagedObject(category = false)
public interface FileBasedConnectionLimitProvider<X extends ConfiguredObject<X>> extends ConfiguredObject<X>
{
    @ManagedAttribute(mandatory = true, description = "File location", oversize = true,
            oversizedAltText = ConfiguredObject.OVER_SIZED_ATTRIBUTE_ALTERNATIVE_TEXT)
    String getPath();

    @ManagedOperation(description = "Causes the connection limit rules to be reloaded. Changes are applied immediately.",
            changesConfiguredObjectState = true)
    void reload();

    @ManagedOperation(nonModifying = true,
            description = "Reset the counters of connections",
            changesConfiguredObjectState = false)
    void resetCounters();

    @DerivedAttribute
    Long getDefaultFrequencyPeriod();

    @DerivedAttribute
    List<ConnectionLimitRule> getRules();
}
