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
package org.apache.qpid.server.query.engine.parsing.converter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.util.Date;

import org.junit.Test;

import org.apache.qpid.server.query.engine.evaluator.EvaluationContext;
import org.apache.qpid.server.query.engine.evaluator.EvaluationContextHolder;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;

public class DateTimeConverterTest
{
    private final DateTimeFormatter _formatter = new DateTimeFormatterBuilder().appendPattern("uuuu-MM-dd HH:mm:ss")
        .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true)
        .toFormatter().withZone(ZoneId.of("UTC")).withResolverStyle(ResolverStyle.STRICT);

    @Test()
    public void isDateTime()
    {
        assertFalse(DateTimeConverter.isDateTime(""));
        assertTrue(DateTimeConverter.isDateTime(new Date()));
        assertTrue(DateTimeConverter.isDateTime(Instant.now()));
        assertTrue(DateTimeConverter.isDateTime(LocalDate.now()));
        assertTrue(DateTimeConverter.isDateTime(LocalDateTime.now()));
    }

    @Test()
    public void toInstant()
    {
        EvaluationContext ctx = EvaluationContextHolder.getEvaluationContext();
        ctx.put(EvaluationContext.QUERY_SETTINGS, new QuerySettings());

        assertNull(DateTimeConverter.toInstantMapper().apply(null));

        Instant expected = LocalDateTime.parse("2020-01-01 00:00:00", _formatter).toInstant(ZoneOffset.UTC);
        Instant actual = DateTimeConverter.toInstantMapper().apply("2020-01-01 00:00:00");

        assertEquals(expected, actual);
    }
}
