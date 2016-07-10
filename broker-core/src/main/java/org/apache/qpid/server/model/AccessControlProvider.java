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
 *
 */
package org.apache.qpid.server.model;

import java.util.Comparator;

import org.apache.qpid.server.security.AccessControl;

@ManagedObject
public interface AccessControlProvider<X extends AccessControlProvider<X>> extends ConfiguredObject<X>, Comparable<AccessControlProvider>
{
    String PRIORITY = "priority";

    Comparator<AccessControlProvider> ACCESS_CONTROL_POVIDER_COMPARATOR = new Comparator<AccessControlProvider>()
    {
        @Override
        public int compare(final AccessControlProvider o1, final AccessControlProvider o2)
        {
            if(o1.getPriority() < o2.getPriority())
            {
                return -1;
            }
            else if (o1.getPriority() > o2.getPriority())
            {
                return 1;
            }
            else
            {
                return o1.getName().compareTo(o2.getName());
            }
        }
    };

    @ManagedAttribute(defaultValue = "10")
    int getPriority();
    //retrieve the underlying AccessControl object
    AccessControl getAccessControl();
}
