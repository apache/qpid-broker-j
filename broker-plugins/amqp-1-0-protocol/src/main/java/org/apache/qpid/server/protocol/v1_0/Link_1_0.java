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
package org.apache.qpid.server.protocol.v1_0;

import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.protocol.LinkModel;
import org.apache.qpid.server.protocol.v1_0.type.BaseSource;
import org.apache.qpid.server.protocol.v1_0.type.BaseTarget;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;

public interface Link_1_0 extends LinkModel
{
    ListenableFuture<LinkEndpoint> attach(Session_1_0 session, final Attach attach);

    void linkClosed();

    void discardEndpoint();

    String getName();

    BaseSource getSource();

    BaseTarget getTarget();

    void setSource(BaseSource source);

    void setTarget(BaseTarget target);

    void setTermini(BaseSource source, BaseTarget target);
}
