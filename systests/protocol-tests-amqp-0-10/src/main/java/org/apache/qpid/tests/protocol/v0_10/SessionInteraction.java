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
package org.apache.qpid.tests.protocol.v0_10;

import org.apache.qpid.server.protocol.v0_10.transport.Method;
import org.apache.qpid.server.protocol.v0_10.transport.SessionAttach;
import org.apache.qpid.server.protocol.v0_10.transport.SessionCommandPoint;
import org.apache.qpid.server.protocol.v0_10.transport.SessionDetach;
import org.apache.qpid.server.protocol.v0_10.transport.SessionFlush;

public class SessionInteraction
{
    private final Interaction _interaction;
    private SessionAttach _attach;
    private SessionDetach _detach;
    private SessionCommandPoint _commandPoint;
    private SessionFlush _flush;

    public SessionInteraction(final Interaction interaction)
    {
        _interaction = interaction;
        _attach = new SessionAttach();
        _detach = new SessionDetach();
        _commandPoint = new SessionCommandPoint();
        _flush = new SessionFlush();
    }

    public Interaction attach() throws Exception
    {
        return _interaction.sendPerformative(_attach);
    }

    public SessionInteraction attachName(final byte[] name)
    {
        _attach.setName(name);
        return this;
    }

    public Interaction detach() throws Exception
    {
        return _interaction.sendPerformative(_detach);
    }

    public SessionInteraction detachName(final byte[] sessionName)
    {
        _detach.setName(sessionName);
        return this;
    }

    public Interaction commandPoint() throws Exception
    {
        return _interaction.sendPerformative(_commandPoint);
    }

    public SessionInteraction commandPointCommandId(final int commandId)
    {
        _commandPoint.setCommandId(commandId);
        return this;
    }

    public Interaction flush() throws Exception
    {
        return _interaction.sendPerformative(_flush);
    }

    public SessionInteraction flushCompleted()
    {
        _flush.setCompleted(true);
        return this;
    }
}
