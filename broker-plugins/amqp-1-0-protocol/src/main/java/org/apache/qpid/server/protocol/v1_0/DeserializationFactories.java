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
 *
 */

package org.apache.qpid.server.protocol.v1_0;

import java.lang.reflect.Array;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.DistributionMode;
import org.apache.qpid.server.protocol.v1_0.type.ErrorCondition;
import org.apache.qpid.server.protocol.v1_0.type.LifetimePolicy;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.TxnCapability;
import org.apache.qpid.server.protocol.v1_0.type.messaging.StdDistMode;
import org.apache.qpid.server.protocol.v1_0.type.transaction.TransactionError;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.ConnectionError;
import org.apache.qpid.server.protocol.v1_0.type.transport.LinkError;
import org.apache.qpid.server.protocol.v1_0.type.transport.SessionError;

public class DeserializationFactories
{
    @SuppressWarnings("unused")
    public static Map<Symbol, Object> convertToNodeProperties(final Object value) throws AmqpErrorException
    {
        if (!(value instanceof Map))
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR,
                                         String.format("Cannot construct 'node-properties' from type '%s'",
                                                       value == null ? null : value.getClass().getSimpleName()));
        }
        Map<Symbol, Object> nodeProperties = new LinkedHashMap<>();
        Map<?, ?> map = (Map<?, ?>) value;
        for (Map.Entry<?, ?> entry : map.entrySet())
        {
            Object key = entry.getKey();
            if (!(key instanceof Symbol))
            {
                throw new AmqpErrorException(AmqpError.DECODE_ERROR,
                                             String.format(
                                                     "'node-properties' must have only keys of type 'symbol' but got '%s'",
                                                     key.getClass().getSimpleName()));
            }
            if (Session_1_0.LIFETIME_POLICY.equals(key))
            {
                final Object lifetimePolicy = entry.getValue();
                if (!(lifetimePolicy instanceof LifetimePolicy))
                {
                    String typeName = lifetimePolicy == null ? null : lifetimePolicy.getClass().getSimpleName();
                    throw new AmqpErrorException(AmqpError.DECODE_ERROR,
                                                 String.format("Cannot construct 'lifetime-policy' from type '%s'",
                                                               typeName));
                }
                nodeProperties.put((Symbol) key, lifetimePolicy);
            }
            else if (Symbol.valueOf("supported-dist-modes").equals(key))
            {
                final Object distributionMode = entry.getValue();
                final DistributionMode[] converted;
                if (distributionMode == null)
                {
                    converted = null;
                }
                else if (distributionMode.getClass().isArray())
                {
                    converted = new DistributionMode[Array.getLength(distributionMode)];
                    for (int i = 0; i < converted.length; ++i)
                    {
                        final Object item = Array.get(distributionMode, i);
                        if (item == null)
                        {
                            throw new AmqpErrorException(AmqpError.DECODE_ERROR,
                                                         "'null' not allowed in 'supported-distribution-modes'");
                        }
                        converted[i] = convertToDistributionMode(item);
                    }
                }
                else
                {
                    converted = new DistributionMode[]{convertToDistributionMode(distributionMode)};
                }
                nodeProperties.put((Symbol) key, converted);
            }
            else
            {
                nodeProperties.put((Symbol) key, entry.getValue());
            }
        }
        return nodeProperties;
    }

    @SuppressWarnings("unused")
    public static DistributionMode convertToDistributionMode(final Object value) throws AmqpErrorException
    {
        if (value instanceof DistributionMode)
        {
            return (DistributionMode) value;
        }
        else if (value instanceof Symbol)
        {
            try
            {
                return StdDistMode.valueOf(value);
            }
            catch (IllegalArgumentException e)
            {
                return new UnknownDistributionMode((Symbol) value);
            }
        }

        final String message = String.format("Cannot construct 'distribution-mode' from type '%s'",
                                             value == null ? null : value.getClass().getSimpleName());
        throw new AmqpErrorException(AmqpError.DECODE_ERROR, message);
    }

    @SuppressWarnings("unused")
    public static TxnCapability convertToTxnCapability(final Object value) throws AmqpErrorException
    {
        if (value instanceof TxnCapability)
        {
            return (TxnCapability) value;
        }
        else if (value instanceof Symbol)
        {
            try
            {
                return org.apache.qpid.server.protocol.v1_0.type.transaction.TxnCapability.valueOf(value);
            }
            catch (IllegalArgumentException e)
            {
                return new UnknownTxnCapability((Symbol) value);
            }
        }

        final String message = String.format("Cannot construct 'txn-capability' from type '%s'",
                                             value == null ? null : value.getClass().getSimpleName());
        throw new AmqpErrorException(AmqpError.DECODE_ERROR, message);
    }

    @SuppressWarnings("unsued")
    public static ErrorCondition convertToErrorCondition(final Object value) throws AmqpErrorException
    {
        if (value instanceof ErrorCondition)
        {
            return (ErrorCondition) value;
        }
        else if (value instanceof Symbol)
        {
            try
            {
                return AmqpError.valueOf(value);
            }
            catch (IllegalArgumentException e)
            {
                try
                {
                    return ConnectionError.valueOf(value);
                }
                catch (IllegalArgumentException e1)
                {
                    try
                    {
                        return SessionError.valueOf(value);
                    }
                    catch (IllegalArgumentException e2)
                    {
                        try
                        {
                            return LinkError.valueOf(value);
                        }
                        catch (IllegalArgumentException e3)
                        {
                            try
                            {
                                return TransactionError.valueOf(value);
                            }
                            catch (IllegalArgumentException e4)
                            {
                                return new UnknownErrorCondition((Symbol) value);
                            }
                        }
                    }
                }
            }
        }
        final String message = String.format("Cannot construct 'error-condition' from type '%s'",
                                             value == null ? null : value.getClass().getSimpleName());
        throw new AmqpErrorException(AmqpError.DECODE_ERROR, message);
    }

    private static final class UnknownErrorCondition implements ErrorCondition
    {
        private final Symbol _value;

        public UnknownErrorCondition(final Symbol value)
        {
            _value = value;
        }

        @Override
        public Symbol getValue()
        {
            return _value;
        }

        @Override
        public boolean equals(final Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }

            final UnknownErrorCondition that = (UnknownErrorCondition) o;

            return _value.equals(that._value);
        }

        @Override
        public int hashCode()
        {
            return _value.hashCode();
        }

        @Override
        public String toString()
        {
            return _value.toString();
        }
    }

    private static class UnknownTxnCapability implements TxnCapability
    {
        private final Symbol _value;

        public UnknownTxnCapability(final Symbol value)
        {
            _value = value;
        }

        @Override
        public boolean equals(final Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }

            final UnknownTxnCapability that = (UnknownTxnCapability) o;

            return _value.equals(that._value);
        }

        @Override
        public int hashCode()
        {
            return _value.hashCode();
        }

        @Override
        public String toString()
        {
            return _value.toString();
        }
    }

    private static class UnknownDistributionMode implements DistributionMode
    {
        private final Symbol _value;

        public UnknownDistributionMode(final Symbol value)
        {
            _value = value;
        }

        @Override
        public boolean equals(final Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }

            final UnknownDistributionMode that = (UnknownDistributionMode) o;

            return _value.equals(that._value);
        }

        @Override
        public int hashCode()
        {
            return _value.hashCode();
        }

        @Override
        public String toString()
        {
            return _value.toString();
        }
    }
}
