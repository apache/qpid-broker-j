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

package org.apache.qpid.server.protocol.v1_0.store.bdb;


import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

import org.apache.qpid.server.protocol.v1_0.LinkKey;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.store.StoreException;

public class LinkKeyEntryBinding extends TupleBinding<LinkKey>
{
    private static final LinkKeyEntryBinding INSTANCE = new LinkKeyEntryBinding();

    private LinkKeyEntryBinding()
    {

    }

    @Override
    public LinkKey entryToObject(final TupleInput input)
    {
        String remoteContainerId =  input.readString();
        String linkName = input.readString();
        Role role = null;
        try
        {
            role = Role.valueOf(input.readBoolean());
        }
        catch (IllegalArgumentException e)
        {
            throw new StoreException("Cannot load link from store", e);
        }

        final String remoteContainerId1 = remoteContainerId;
        final String linkName1 = linkName;
        final Role role1 = role;
        return new LinkKey(remoteContainerId1, linkName1, role1);
    }

    @Override
    public void objectToEntry(final LinkKey linkKey, final TupleOutput output)
    {
        output.writeString(linkKey.getRemoteContainerId());
        output.writeString(linkKey.getLinkName());
        output.writeBoolean(linkKey.getRole().getValue());
    }

    public static LinkKeyEntryBinding getInstance()
    {
        return INSTANCE;
    }
}
