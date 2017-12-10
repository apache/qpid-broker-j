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
package org.apache.qpid.tests.protocol.v0_8;

import org.apache.qpid.server.protocol.v0_8.transport.TxCommitBody;
import org.apache.qpid.server.protocol.v0_8.transport.TxRollbackBody;
import org.apache.qpid.server.protocol.v0_8.transport.TxSelectBody;
import org.apache.qpid.tests.protocol.AbstractInteraction;

public class TxInteraction
{
    private Interaction _interaction;

    public TxInteraction(final Interaction interaction)
    {
        _interaction = interaction;
    }

    public Interaction select() throws Exception
    {
        return _interaction.sendPerformative(TxSelectBody.INSTANCE);
    }

    public Interaction commit() throws Exception
    {
        return _interaction.sendPerformative(TxCommitBody.INSTANCE);
    }

    public Interaction rollback() throws Exception
    {
        return _interaction.sendPerformative(TxRollbackBody.INSTANCE);
    }
}
