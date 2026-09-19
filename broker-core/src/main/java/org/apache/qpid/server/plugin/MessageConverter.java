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
package org.apache.qpid.server.plugin;

import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.protocol.converter.MessageConversionException;
import org.apache.qpid.server.util.GZIPUtils;
import org.apache.qpid.server.util.GZIPUtils.GZIPInflationLimitException;

public interface MessageConverter<M extends ServerMessage, N extends ServerMessage> extends Pluggable
{
    Class<M> getInputClass();
    Class<N> getOutputClass();

    N convert(final M message, final NamedAddressSpace addressSpace);

    default N convert(final M message,
                      final NamedAddressSpace addressSpace,
                      final int maximumMessageDecompressionSize)
    {
        if (message.getMessageHeader() != null &&
                GZIPUtils.GZIP_CONTENT_ENCODING.equals(message.getMessageHeader().getEncoding()))
        {
            final String description = String.format("Message converter '%s' cannot guarantee the %d byte " +
                    "decompression limit for gzip content", getType(), maximumMessageDecompressionSize);
            throw new MessageConversionException(description, new GZIPInflationLimitException(description));
        }
        return convert(message, addressSpace);
    }

    void dispose(N message);
}
