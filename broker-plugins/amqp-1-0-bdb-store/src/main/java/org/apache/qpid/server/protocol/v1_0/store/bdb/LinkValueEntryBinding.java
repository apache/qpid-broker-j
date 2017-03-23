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

import org.apache.qpid.server.protocol.v1_0.store.LinkStoreUtils;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.store.StoreException;

public class LinkValueEntryBinding extends TupleBinding<LinkValue>
{
    private static final LinkValueEntryBinding INSTANCE = new LinkValueEntryBinding();

    private LinkValueEntryBinding()
    {
    }

    @Override
    public LinkValue entryToObject(final TupleInput input)
    {
        byte version = input.readByte();
        Object source = read(input);
        if (!(source instanceof Source))
        {
            throw new StoreException(String.format("Unexpected object '%s' stored in the store where Source is expected",
                                                   source.getClass()));
        }

        Object target = read(input);
        if (!(target instanceof Target))
        {
            throw new StoreException(String.format("Unexpected object '%s' stored in the store where Target is expected",
                                                   target.getClass()));
        }

        return new LinkValue((Source) source, (Target)target, version);
    }


    @Override
    public void objectToEntry(final LinkValue linkValue, final TupleOutput output)
    {
        output.writeByte(linkValue.getVersion());
        write(linkValue.getSource(), output);
        write(linkValue.getTarget(), output);
    }

    public static LinkValueEntryBinding getInstance()
    {
        return INSTANCE;
    }

    private Object read(final TupleInput input)
    {
        int size = input.readInt();
        byte[] bytes = new byte[size];
        input.read(bytes);

        return LinkStoreUtils.amqpBytesToObject(bytes);
    }



    private void write(final Object object, final TupleOutput output)
    {
        byte[] bytes = LinkStoreUtils.objectToAmqpBytes(object);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

}
