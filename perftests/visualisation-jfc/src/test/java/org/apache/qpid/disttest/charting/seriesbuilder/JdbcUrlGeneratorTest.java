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
package org.apache.qpid.disttest.charting.seriesbuilder;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Assert;

import org.apache.qpid.disttest.charting.definition.SeriesDefinition;

import org.junit.Assert;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertNotNull;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class JdbcUrlGeneratorTest extends UnitTestBase
{
    @Test
    public void testGetJdbcUrlWithoutProvidingAUrlReturnsCsvUrlWithCorrectDirectory()
    {
        JdbcUrlGenerator jdbcUrlGenerator = new JdbcUrlGenerator(null);
        SeriesDefinition seriesDefinition = mock(SeriesDefinition.class);
        when(seriesDefinition.getSeriesDirectory()).thenReturn("mydir");

        String jdbcUrl = jdbcUrlGenerator.getJdbcUrl(seriesDefinition);

        assertEquals("jdbc:relique:csv:mydir", jdbcUrl);
    }

    @Test
    public void testGetJdbcUrlReturnsProvidedUrl()
    {
        String urlTemplate = "urlTemplate";
        JdbcUrlGenerator jdbcUrlGenerator = new JdbcUrlGenerator(urlTemplate);
        SeriesDefinition seriesDefinition = mock(SeriesDefinition.class);

        String jdbcUrl = jdbcUrlGenerator.getJdbcUrl(seriesDefinition);

        assertEquals(urlTemplate, jdbcUrl);
    }

    @Test
    public void testGetJdbcUrlThrowsExceptionIfUrlProvidedAndSeriesDirectorySpecified()
    {
        String urlTemplate = "urlTemplate";
        JdbcUrlGenerator jdbcUrlGenerator = new JdbcUrlGenerator(urlTemplate);
        SeriesDefinition seriesDefinition = mock(SeriesDefinition.class);
        when(seriesDefinition.getSeriesDirectory()).thenReturn("mydir");

        try
        {
            jdbcUrlGenerator.getJdbcUrl(seriesDefinition);
            fail("Expected exception not thrown");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
    }

    @Test
    public void testGetJdbcUrlThrowsExceptionWithoutAProvidedUrlOrSeriesDirectory()
    {
        JdbcUrlGenerator jdbcUrlGenerator = new JdbcUrlGenerator(null);
        SeriesDefinition seriesDefinition = mock(SeriesDefinition.class);
        when(seriesDefinition.getSeriesDirectory()).thenReturn(null);

        try
        {
            jdbcUrlGenerator.getJdbcUrl(seriesDefinition);
            fail("Expected exception not thrown");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
    }
}
