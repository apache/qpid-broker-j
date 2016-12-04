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
package org.apache.qpid.server.security.access.plugins;


import java.util.List;

import org.apache.qpid.server.model.Content;
import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedOperation;
import org.apache.qpid.server.model.Param;
import org.apache.qpid.server.model.VirtualHostAccessControlProvider;
import org.apache.qpid.server.security.Result;

@ManagedObject( category = false, type= RuleBasedVirtualHostAccessControlProvider.RULE_BASED_TYPE, amqpName = "org.apache.qpid.RuleBaseVirtualHostAccessControlProvider")
public interface RuleBasedVirtualHostAccessControlProvider<X extends RuleBasedVirtualHostAccessControlProvider<X>> extends VirtualHostAccessControlProvider<X>
{
    String RULE_BASED_TYPE = "RuleBased";
    String DEFAULT_RESULT= "defaultResult";
    String RULES = "rules";

    @ManagedAttribute( mandatory = true, defaultValue = "DEFER", validValues = { "ALLOWED", "DENIED", "DEFER" })
    Result getDefaultResult();

    @ManagedAttribute( mandatory = true, defaultValue = "[ ]")
    List<AclRule> getRules();

    @ManagedOperation(changesConfiguredObjectState = true)
    void loadFromFile(@Param(name = "path", mandatory = true) String path);

    @ManagedOperation(nonModifying = true, changesConfiguredObjectState = false)
    Content extractRules();
}
