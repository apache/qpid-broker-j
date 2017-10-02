
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

import java.util.Map;

import org.apache.qpid.server.protocol.v1_0.codec.AbstractDescribedTypeConstructor;
import org.apache.qpid.server.protocol.v1_0.codec.DescribedTypeConstructorRegistry;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedLong;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DeliveryAnnotations;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;

public class DeliveryAnnotationsConstructor extends AbstractDescribedTypeConstructor<DeliveryAnnotations>
{
    private static final Object[] DESCRIPTORS =
    {
            Symbol.valueOf("amqp:delivery-annotations:map"),UnsignedLong.valueOf(0x0000000000000071L),
    };

    private static final DeliveryAnnotationsConstructor INSTANCE = new DeliveryAnnotationsConstructor();

    public static void register(DescribedTypeConstructorRegistry registry)
    {
        for(Object descriptor : DESCRIPTORS)
        {
            registry.register(descriptor, INSTANCE);
        }
    }


    @Override
    public DeliveryAnnotations construct(Object underlying) throws AmqpErrorException
    {

        if(underlying instanceof Map)
        {
            return new DeliveryAnnotations((Map)underlying);
        }
        else
        {
            final String msg = String.format("Cannot decode 'delivery-annotations' from '%s'",
                                             underlying == null ? null : underlying.getClass().getSimpleName());
            throw new AmqpErrorException(AmqpError.DECODE_ERROR, msg);
        }
    }


}
