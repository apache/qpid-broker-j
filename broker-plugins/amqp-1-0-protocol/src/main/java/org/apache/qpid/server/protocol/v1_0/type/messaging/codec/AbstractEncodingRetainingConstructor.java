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
package org.apache.qpid.server.protocol.v1_0.type.messaging.codec;

import java.util.ArrayList;
import java.util.List;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.codec.DescribedTypeConstructor;
import org.apache.qpid.server.protocol.v1_0.codec.TypeConstructor;
import org.apache.qpid.server.protocol.v1_0.codec.ValueHandler;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;

abstract public class AbstractEncodingRetainingConstructor<T extends EncodingRetaining> implements DescribedTypeConstructor<T>
{
    @Override
    public TypeConstructor<T> construct(final Object descriptor,
                                        final List<QpidByteBuffer> in,
                                        final int[] originalPositions,
                                        final ValueHandler valueHandler)
            throws AmqpErrorException
    {
        return new TypeConstructorFromUnderlying<>(this, valueHandler.readConstructor(in), originalPositions);
    }

    abstract protected T construct(Object underlying);

    private static class TypeConstructorFromUnderlying<S extends EncodingRetaining> implements TypeConstructor<S>
    {

        private final TypeConstructor _describedConstructor;
        private final int[] _originalPositions;
        private AbstractEncodingRetainingConstructor<S> _describedTypeConstructor;

        public TypeConstructorFromUnderlying(final AbstractEncodingRetainingConstructor<S> describedTypeConstructor,
                                             final TypeConstructor describedConstructor, final int[] originalPositions)
        {
            _describedConstructor = describedConstructor;
            _describedTypeConstructor = describedTypeConstructor;
            _originalPositions = originalPositions;
        }

        @Override
        public S construct(final List<QpidByteBuffer> in, final ValueHandler handler) throws AmqpErrorException
        {

            S object =  _describedTypeConstructor.construct(_describedConstructor.construct(in, handler));
            List<QpidByteBuffer> encoding = new ArrayList<>();
            int offset = in.size() - _originalPositions.length;
            for(int i = offset; i < in.size(); i++)
            {
                QpidByteBuffer buf = in.get(i);
                if(buf.position() == _originalPositions[i-offset])
                {
                    if(buf.hasRemaining())
                    {
                        break;
                    }
                }
                else
                {
                    QpidByteBuffer dup = buf.duplicate();
                    dup.position(_originalPositions[i-offset]);
                    dup.limit(buf.position());
                    encoding.add(dup);
                }
            }
            object.setEncodedForm(encoding);
            return object;
        }
    }


}
