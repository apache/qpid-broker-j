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

import org.apache.qpid.server.protocol.v0_10.transport.TxCommit;
import org.apache.qpid.server.protocol.v0_10.transport.TxSelect;

public class TxInteraction
{
    private final Interaction _interaction;
    private final TxSelect _select;
    private final TxCommit _commit;

    public TxInteraction(final Interaction interaction)
    {
        _interaction = interaction;
        _select = new TxSelect();
        _commit = new TxCommit();
    }

    public Interaction select() throws Exception
    {
        return _interaction.sendPerformative(_select);
    }

    public TxInteraction selectId(final int id)
    {
        _select.setId(id);
        return this;
    }

    public TxInteraction commitId(final int id)
    {
        _commit.setId(id);
        return this;
    }

    public Interaction commit() throws Exception
    {
        return _interaction.sendPerformative(_commit);
    }
}
