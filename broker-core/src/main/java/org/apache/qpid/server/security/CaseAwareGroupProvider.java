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
package org.apache.qpid.server.security;

import org.apache.qpid.server.model.GroupProvider;
import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.model.ManagedContextDefault;

public interface CaseAwareGroupProvider<X extends GroupProvider<X>> extends GroupProvider<X>
{
    String GROUP_PROVIDER_CASE_SENSITIVE = "qpid.groupProvider.caseSensitive";
    @SuppressWarnings("unused")
    @ManagedContextDefault(name = GROUP_PROVIDER_CASE_SENSITIVE)
    String DEFAULT_GROUP_PROVIDER_CASE_SENSITIVE = "true";

    @ManagedAttribute(defaultValue = "${" + GROUP_PROVIDER_CASE_SENSITIVE + "}",
            description = "Allow to choose CaseSensitive or CaseInsensitive search of Groups and Users")
    boolean isCaseSensitive();
}
