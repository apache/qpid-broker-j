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

package org.apache.qpid.server.protocol.v1_0.codec;

import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;

final class CodecValidation
{
    private CodecValidation()
    {
    }

    static void validateCountAndSize(final int count, final int size, final String typeName)
            throws AmqpErrorException
    {
        if (size < 0)
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Invalid negative size %d for '%s'", size, typeName);
        }
        if (count < 0)
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Invalid negative element count %d for '%s'", count,
                    typeName);
        }
        if (count > size)
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Element count %d exceeds encoded size %d for '%s'",
                    count, size, typeName);
        }
    }

    static boolean isZeroWidthArrayElementConstructor(final TypeConstructor<?> constructor)
    {
        return constructor.isZeroWidthArrayElement();
    }
}
