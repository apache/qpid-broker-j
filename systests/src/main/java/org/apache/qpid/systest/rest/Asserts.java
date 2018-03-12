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
package org.apache.qpid.systest.rest;

import java.util.Map;

import junit.framework.TestCase;

import org.apache.qpid.server.model.ConfiguredObject;

public class Asserts
{
    public static final String STATISTICS_ATTRIBUTE = "statistics";


    public static void assertActualAndDesiredState(final String expectedDesiredState,
                                             final String expectedActualState,
                                             final Map<String, Object> data)
    {
        String name = (String) data.get(ConfiguredObject.NAME);
        TestCase.assertEquals("Object with name " + name + " has unexpected desired state",
                              expectedDesiredState,
                              data.get(ConfiguredObject.DESIRED_STATE));
        TestCase.assertEquals("Object with name " + name + " has unexpected actual state",
                              expectedActualState, data.get(ConfiguredObject.STATE));
    }
}
