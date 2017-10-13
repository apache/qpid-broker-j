/*
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
 */

package org.apache.qpid.server.transport;

import java.io.IOException;
import java.security.Principal;
import java.security.cert.Certificate;
import java.util.Collection;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;

interface NonBlockingConnectionDelegate
{
    class WriteResult
    {
        final boolean _complete;
        final long _bytesConsumed;

        public WriteResult(final boolean complete, final long bytesConsumed)
        {
            _complete = complete;
            _bytesConsumed = bytesConsumed;
        }

        public boolean isComplete()
        {
            return _complete;
        }

        public long getBytesConsumed()
        {
            return _bytesConsumed;
        }
    }

    WriteResult doWrite(Collection<QpidByteBuffer> buffers) throws IOException;

    boolean readyForRead();

    boolean processData() throws IOException;

    Principal getPeerPrincipal();

    Certificate getPeerCertificate();

    boolean needsWork();

    QpidByteBuffer getNetInputBuffer();

    void shutdownInput();

    void shutdownOutput();

    String getTransportInfo();
}
