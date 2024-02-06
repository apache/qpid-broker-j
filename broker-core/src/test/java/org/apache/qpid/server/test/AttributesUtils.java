/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.qpid.server.test;

import java.util.HashMap;
import java.util.Map;

public class AttributesUtils
{
    /** Creates new attributes map along with the new context map */
    public static Map<String, Object> createAttributesMap(final Map<String, Object> attributes,
                                                          final Map<String, String> context)
    {
        final Map<String, Object> copied = new HashMap<>(attributes);
        copied.put("context", new HashMap<>(context));
        return copied;
    }
}
