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

package org.apache.qpid.test.utils;

import java.util.HashMap;
import java.util.Map;

import org.junit.rules.ExternalResource;

public class SystemPropertySetter extends ExternalResource
{
    private Map<String, String> _storedProperties = new HashMap<>();

    @Override
    public synchronized void after()
    {
        _storedProperties.forEach(this::setProperty);
    }

    public synchronized void setSystemProperty(final String name, final String value)
    {
        _storedProperties.putIfAbsent(name, System.getProperty(name));
        setProperty(name, value);
    }

    private void setProperty(final String name, final String value)
    {
        if (value == null)
        {
            System.clearProperty(name);
        }
        else
        {
            System.setProperty(name, value);
        }
    }
}
