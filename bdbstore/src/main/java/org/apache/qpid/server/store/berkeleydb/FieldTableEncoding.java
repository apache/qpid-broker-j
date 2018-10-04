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
package org.apache.qpid.server.store.berkeleydb;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.DatabaseException;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.FieldTableFactory;
import org.apache.qpid.server.store.berkeleydb.tuple.ByteBufferBinding;

import java.nio.ByteBuffer;

public class FieldTableEncoding
{
    private FieldTableEncoding()
    {
    }

    public static FieldTable readFieldTable(TupleInput tupleInput) throws DatabaseException
    {

        long length = tupleInput.readLong();
        if (length <= 0)
        {
            return null;
        }
        else
        {

            ByteBuffer buf = ByteBufferBinding.getInstance().readByteBuffer(tupleInput, (int) length);

            return FieldTableFactory.createFieldTable(QpidByteBuffer.wrap(buf));

        }

    }

    public  static void writeFieldTable(FieldTable fieldTable, TupleOutput tupleOutput)
    {

        if (fieldTable == null)
        {
            tupleOutput.writeLong(0);
        }
        else
        {
            tupleOutput.writeLong(fieldTable.getEncodedSize());
            tupleOutput.writeFast(fieldTable.getDataAsBytes());
        }
    }
}
