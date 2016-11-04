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

public abstract class AbstractConfigurationChangeListener implements ConfigurationChangeListener
{
    @Override
    public void stateChanged(final ConfiguredObject<?> object, final State oldState, final State newState)
    {

    }

    @Override
    public void childAdded(final ConfiguredObject<?> object, final ConfiguredObject<?> child)
    {

    }

    @Override
    public void childRemoved(final ConfiguredObject<?> object, final ConfiguredObject<?> child)
    {

    }

    @Override
    public void attributeSet(final ConfiguredObject<?> object,
                             final String attributeName,
                             final Object oldAttributeValue,
                             final Object newAttributeValue)
    {

    }

    @Override
    public void bulkChangeStart(final ConfiguredObject<?> object)
    {

    }

    @Override
    public void bulkChangeEnd(final ConfiguredObject<?> object)
    {

    }
}
