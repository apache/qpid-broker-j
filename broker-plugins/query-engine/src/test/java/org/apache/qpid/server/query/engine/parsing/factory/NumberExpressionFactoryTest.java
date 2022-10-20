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
package org.apache.qpid.server.query.engine.parsing.factory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.math.BigDecimal;

import org.junit.Test;

import org.apache.qpid.server.query.engine.evaluator.EvaluationContext;
import org.apache.qpid.server.query.engine.evaluator.EvaluationContextHolder;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;

/**
 * Tests designed to verify the {@link NumberExpressionFactory} functionality
 */
public class NumberExpressionFactoryTest
{

    @Test()
    public void fromDecimal()
    {
        EvaluationContextHolder.getEvaluationContext().put(EvaluationContext.QUERY_SETTINGS, new QuerySettings());

        assertNull(NumberExpressionFactory.fromDecimal(null));
        assertEquals(0, NumberExpressionFactory.fromDecimal("0"));
        assertEquals(10, NumberExpressionFactory.fromDecimal("10"));
        assertEquals(100, NumberExpressionFactory.fromDecimal("100"));
        assertEquals(1000, NumberExpressionFactory.fromDecimal("1000"));
        assertEquals(10_000, NumberExpressionFactory.fromDecimal("10000"));
        assertEquals(100_000, NumberExpressionFactory.fromDecimal("100000"));
        assertEquals(1000_000, NumberExpressionFactory.fromDecimal("1000000"));
        assertEquals(10_000_000, NumberExpressionFactory.fromDecimal("10000000"));
        assertEquals(100_000_000, NumberExpressionFactory.fromDecimal("100000000"));
        assertEquals(1000_000_000, NumberExpressionFactory.fromDecimal("1000000000"));
        assertEquals(10_000_000_000L, NumberExpressionFactory.fromDecimal("10000000000"));
        assertEquals(100_000_000_000L, NumberExpressionFactory.fromDecimal("100000000000"));
        assertEquals(1000_000_000_000L, NumberExpressionFactory.fromDecimal("1000000000000"));
        assertEquals(10_000_000_000_000L, NumberExpressionFactory.fromDecimal("10000000000000"));
        assertEquals(100_000_000_000_000L, NumberExpressionFactory.fromDecimal("100000000000000"));
        assertEquals(1000_000_000_000_000L, NumberExpressionFactory.fromDecimal("1000000000000000"));
        assertEquals(BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE), NumberExpressionFactory.fromDecimal(BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE).toString()));
        assertEquals(BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.ONE), NumberExpressionFactory.fromDecimal(BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.ONE).toString()));
    }

    @Test()
    public void fromHex()
    {
        EvaluationContextHolder.getEvaluationContext().put(EvaluationContext.QUERY_SETTINGS, new QuerySettings());

        assertNull(NumberExpressionFactory.fromHex(null));
        assertEquals(0x0, NumberExpressionFactory.fromHex("0x0"));
        assertEquals(0x10, NumberExpressionFactory.fromHex("0x10"));
        assertEquals(0x100, NumberExpressionFactory.fromHex("0x100"));
        assertEquals(0x1000, NumberExpressionFactory.fromHex("0x1000"));
        assertEquals(0x10_000, NumberExpressionFactory.fromHex("0x10000"));
        assertEquals(0x100_000, NumberExpressionFactory.fromHex("0x100000"));
        assertEquals(0x1000_000, NumberExpressionFactory.fromHex("0x1000000"));
        assertEquals(0x10_000_000, NumberExpressionFactory.fromHex("0x10000000"));
        assertEquals(0x100_000_000L, NumberExpressionFactory.fromHex("0x100000000"));
        assertEquals(0x1000_000_000L, NumberExpressionFactory.fromHex("0x1000000000"));
        assertEquals(0x10_000_000_000L, NumberExpressionFactory.fromHex("0x10000000000"));
        assertEquals(0x100_000_000_000L, NumberExpressionFactory.fromHex("0x100000000000"));
        assertEquals(0x1000_000_000_000L, NumberExpressionFactory.fromHex("0x1000000000000"));
        assertEquals(0x10_000_000_000_000L, NumberExpressionFactory.fromHex("0x10000000000000"));
        assertEquals(0x100_000_000_000_000L, NumberExpressionFactory.fromHex("0x100000000000000"));
        assertEquals(0x1000_000_000_000_000L, NumberExpressionFactory.fromHex("0x1000000000000000"));
    }

    @Test()
    public void fromOctal()
    {
        EvaluationContextHolder.getEvaluationContext().put(EvaluationContext.QUERY_SETTINGS, new QuerySettings());

        assertNull(NumberExpressionFactory.fromHex(null));
        assertEquals(0, NumberExpressionFactory.fromOctal("00"));
        assertEquals(8, NumberExpressionFactory.fromOctal("010"));
        assertEquals(64, NumberExpressionFactory.fromOctal("0100"));
        assertEquals(512, NumberExpressionFactory.fromOctal("01000"));
        assertEquals(4096, NumberExpressionFactory.fromOctal("010000"));
        assertEquals(32768, NumberExpressionFactory.fromOctal("0100000"));
        assertEquals(262144, NumberExpressionFactory.fromOctal("01000000"));
        assertEquals(2097152, NumberExpressionFactory.fromOctal("010000000"));
        assertEquals(16777216, NumberExpressionFactory.fromOctal("0100000000"));
        assertEquals(134217728, NumberExpressionFactory.fromOctal("01000000000"));
        assertEquals(1073741824, NumberExpressionFactory.fromOctal("010000000000"));
        assertEquals(8589934592L, NumberExpressionFactory.fromOctal("0100000000000"));
        assertEquals(68719476736L, NumberExpressionFactory.fromOctal("01000000000000"));
        assertEquals(549755813888L, NumberExpressionFactory.fromOctal("010000000000000"));
    }
}
