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

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;

public abstract class AbstractDescribedTypeConstructor<T> implements DescribedTypeConstructor<T>
{
    @Override
    public TypeConstructor<T> construct(final Object descriptor,
                                        final QpidByteBuffer in,
                                        final int originalPosition, final ValueHandler valueHandler)
            throws AmqpErrorException
    {
        final TypeConstructor constructor = allowsDescribedTypeValue()
                ? valueHandler.readConstructorAllowingDescribedTypeValue(in)
                : valueHandler.readNonDescribedConstructor(in);
        return new TypeConstructorFromUnderlying<>(this, constructor, allowsDescribedTypeValue());
    }

    protected abstract T construct(Object underlying) throws AmqpErrorException;

    private static class TypeConstructorFromUnderlying<S> implements TypeConstructor<S>
    {
        private final TypeConstructor _describedConstructor;
        private final AbstractDescribedTypeConstructor<S> _describedTypeConstructor;
        private final boolean _allowsDescribedTypeValue;

        public TypeConstructorFromUnderlying(final AbstractDescribedTypeConstructor<S> describedTypeConstructor,
                                             final TypeConstructor describedConstructor,
                                             final boolean allowsDescribedTypeValue)
        {
            _describedConstructor = describedConstructor;
            _describedTypeConstructor = describedTypeConstructor;
            _allowsDescribedTypeValue = allowsDescribedTypeValue;
        }

        @Override
        public S construct(final QpidByteBuffer in, final ValueHandler handler) throws AmqpErrorException
        {
            final Object underlying = _allowsDescribedTypeValue
                    ? handler.parseWithConstructorAllowingDescribedTypeValue(in, _describedConstructor)
                    : handler.parseWithConstructor(in, _describedConstructor);
            return _describedTypeConstructor.construct(underlying);
        }

        @Override
        public boolean isZeroWidthArrayElement()
        {
            return _describedConstructor.isZeroWidthArrayElement();
        }
    }
}
