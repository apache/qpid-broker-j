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
package org.apache.qpid.server.model;

import java.util.Comparator;

@ManagedObject( creatable = false )
public interface VirtualHostAlias<X extends VirtualHostAlias<X>> extends ConfiguredObject<X>
{
    Comparator<VirtualHostAlias> COMPARATOR = new Comparator<VirtualHostAlias>()
    {
        @Override
        public int compare(final VirtualHostAlias left, final VirtualHostAlias right)
        {
            int comparison = left.getPriority() - right.getPriority();
            if (comparison == 0)
            {
                long leftTime = left.getCreatedTime() == null ? 0 : left.getCreatedTime().getTime();
                long rightTime = right.getCreatedTime() == null ? 0 : right.getCreatedTime().getTime();
                long createCompare = leftTime - rightTime;
                if (createCompare == 0)
                {
                    comparison = left.getName().compareTo(right.getName());
                }
                else
                {
                    comparison = createCompare < 0L ? -1 : 1;
                }
            }
            return comparison;
        }
    };

    String PRIORITY = "priority";
    // parents
    Port getPort();

    @ManagedAttribute( defaultValue = "100" )
    int getPriority();


    NamedAddressSpace getAddressSpace(String name);

}
