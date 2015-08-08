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

package org.apache.qpid.server.model.testmodels.hierarchy;

import java.util.Map;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObject;

public class TestAbstractEngineImpl<X extends TestAbstractEngineImpl<X>> extends AbstractConfiguredObject<X> implements TestEngine<X>
{
    private ListenableFuture<Void> _beforeCloseFuture = Futures.immediateFuture(null);

    public TestAbstractEngineImpl(final Map<Class<? extends ConfiguredObject>, ConfiguredObject<?>> parents,
                                  final Map<String, Object> attributes)
    {
        super(parents, attributes);
    }

    @Override
    public void setBeforeCloseFuture(final ListenableFuture listenableFuture)
    {
        _beforeCloseFuture = listenableFuture;
    }

    @Override
    protected ListenableFuture<Void> beforeClose()
    {
        return _beforeCloseFuture;
    }
}
