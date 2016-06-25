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

import org.apache.qpid.server.model.AccessControlProvider;
import org.apache.qpid.server.model.Content;
import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedOperation;
import org.apache.qpid.server.model.Param;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.security.access.RuleOutcome;

@ManagedObject( category = false, type= RuleBasedAccessControlProvider.RULE_BASED_TYPE)
public interface RuleBasedAccessControlProvider<X extends RuleBasedAccessControlProvider<X>> extends AccessControlProvider<X>
{
    String RULE_BASED_TYPE = "RuleBased";
    String DEFAULT_RESULT= "defaultResult";
    String RULES = "rules";

    @ManagedAttribute( mandatory = true, defaultValue = "DENIED", validValues = { "ALLOWED", "DENIED" })
    Result getDefaultResult();

    @ManagedAttribute( mandatory = true, defaultValue = "[ { \"identity\" : \"ALL\", \"objectType\" : \"ALL\", \"operation\" : \"ALL\", \"attributes\" : {}, \"outcome\" : \"ALLOW\"} ]")
    List<AclRule> getRules();

    @ManagedOperation
    void loadFromFile(@Param(name = "path")String path);

    @ManagedOperation(nonModifying = true)
    Content extractRules();
}
