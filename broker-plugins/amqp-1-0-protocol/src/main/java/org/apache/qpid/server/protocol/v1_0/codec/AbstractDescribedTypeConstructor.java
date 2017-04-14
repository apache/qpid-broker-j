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

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.CompositeTypeField;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;

public abstract class AbstractDescribedTypeConstructor<T extends Object> implements DescribedTypeConstructor<T>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDescribedTypeConstructor.class);

    @Override
    public TypeConstructor<T> construct(final Object descriptor,
                                        final List<QpidByteBuffer> in,
                                        final int[] originalPositions, final ValueHandler valueHandler)
            throws AmqpErrorException
    {

        return new TypeConstructorFromUnderlying<>(this, valueHandler.readConstructor(in));
    }

    protected abstract T construct(Object underlying);

    private static class TypeConstructorFromUnderlying<S extends Object> implements TypeConstructor<S>
    {

        private final TypeConstructor _describedConstructor;
        private AbstractDescribedTypeConstructor<S> _describedTypeConstructor;
        private static final Map<Class<?>, CompositeTypeValidator> _validators = new ConcurrentHashMap<>();

        public TypeConstructorFromUnderlying(final AbstractDescribedTypeConstructor<S> describedTypeConstructor,
                                             final TypeConstructor describedConstructor)
        {
            _describedConstructor = describedConstructor;
            _describedTypeConstructor = describedTypeConstructor;
        }

        @Override
        public S construct(final List<QpidByteBuffer> in, final ValueHandler handler) throws AmqpErrorException
        {
            final S constructedObject =
                    _describedTypeConstructor.construct(_describedConstructor.construct(in, handler));
            CompositeTypeValidator<S> validator =
                    _validators.computeIfAbsent(constructedObject.getClass(), k -> createValidator(constructedObject));
            validator.validate(constructedObject);
            return constructedObject;
        }

        private CompositeTypeValidator<S> createValidator(final S constructedObject)
        {
            final List<Field> mandatoryFields = new ArrayList<>();
            for (Field field : constructedObject.getClass().getDeclaredFields())
            {
                Annotation[] annotations = field.getDeclaredAnnotationsByType(CompositeTypeField.class);
                for (Annotation annotation : annotations)
                {
                    if (annotation instanceof CompositeTypeField && ((CompositeTypeField) annotation).mandatory())
                    {
                        field.setAccessible(true);
                        mandatoryFields.add(field);
                    }
                }
            }
            return objectToValidate ->
            {
                try
                {
                    if (!mandatoryFields.isEmpty())
                    {
                        for (Field field : mandatoryFields)
                        {
                            if (field.get(objectToValidate) == null)
                            {
                                throw new AmqpErrorException(AmqpError.DECODE_ERROR,
                                                             String.format("Missing mandatory field '%s'.",
                                                                           field.getName()));
                            }
                        }
                    }
                }
                catch (IllegalAccessException e)
                {
                    LOGGER.error(String.format("Error validating AMQP 1.0 object '%s'", constructedObject.toString()), e);
                    throw new AmqpErrorException(AmqpError.INTERNAL_ERROR, "Failure during object validation");
                }
            };
        }

        private interface CompositeTypeValidator<S>
        {
            void validate(final S constructedObject) throws AmqpErrorException;
        }
    }
}
