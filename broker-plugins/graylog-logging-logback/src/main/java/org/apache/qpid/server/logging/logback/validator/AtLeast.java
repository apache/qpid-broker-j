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

package org.apache.qpid.server.logging.logback.validator;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.ConfiguredObject;

import java.util.function.Predicate;

public class AtLeast implements Validator<Integer>, Predicate<Integer>
{
    private final int _min;

    AtLeast(int min)
    {
        this._min = min;
    }

    @Override
    public boolean test(Integer value)
    {
        return value != null && value >= _min;
    }

    @Override
    public void validate(Integer value, ConfiguredObject<?> object, String attribute)
    {
        if (!test(value))
        {
            throw new IllegalConfigurationException(errorMessage(value, object, attribute));
        }
    }

    private String errorMessage(Integer value, ConfiguredObject<?> object, String attribute)
    {
        return "Attribute '" + attribute
                + "' instance of " + object.getClass().getName()
                + " named '" + object.getName() + "'"
                + " cannot have value '" + value + "'"
                + " as it has to be at least " + minimum();
    }

    int minimum()
    {
        return _min;
    }
}
