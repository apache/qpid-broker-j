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
package org.apache.qpid.server.user.connection.limits.config;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;

import com.google.common.collect.Iterables;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.test.utils.UnitTestBase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FileParserTest extends UnitTestBase
{
    private static final String FILE_SUFFIX = "clt";
    private static final Charset CHARSET = StandardCharsets.UTF_8;

    @Test
    public void testParseRule_ConnectionCountLimit()
    {
        final RuleSetCreator creator = writeConfig("CLT user port=amqp connection_limit=10");

        final Rule rule = Iterables.getFirst(creator, null);
        assertNotNull(rule);
        assertEquals("user", rule.getIdentity());
        assertEquals("amqp", rule.getPort());
        assertFalse(rule.isUserBlocked());
        assertEquals(Integer.valueOf(10), rule.getCountLimit());
        assertNotNull(rule.getFrequencyLimits());
        assertNull(rule.getFrequencyPeriod());
        assertNull(rule.getFrequencyLimit());
        assertTrue(rule.getFrequencyLimits().isEmpty());
    }

    @Test
    public void testParseRule_ConnectionFrequencyLimit()
    {
        final RuleSetCreator creator = writeConfig("CLT user port=amqp connection-frequency-limit=10/15M");

        final Rule rule = Iterables.getFirst(creator, null);
        assertNotNull(rule);
        assertEquals("user", rule.getIdentity());
        assertEquals("amqp", rule.getPort());
        assertFalse(rule.isUserBlocked());
        assertEquals(Collections.singletonMap(Duration.ofMinutes(15L), 10), rule.getFrequencyLimits());
        assertEquals(Duration.ofMinutes(15L), rule.getFrequencyPeriod());
        assertEquals(Integer.valueOf(10), rule.getFrequencyLimit());
        assertNull(rule.getCountLimit());
    }

    @Test
    public void testParseRule_ConnectionFrequencyLimit_1minute()
    {
        final RuleSetCreator creator = writeConfig("CLT user port=amqp connection-frequency-limit=17/m");

        final Rule rule = Iterables.getFirst(creator, null);
        assertNotNull(rule);
        assertEquals("user", rule.getIdentity());
        assertEquals("amqp", rule.getPort());
        assertFalse(rule.isUserBlocked());
        assertEquals(Collections.singletonMap(Duration.ofMinutes(1L), 17), rule.getFrequencyLimits());
        assertEquals(Duration.ofMinutes(1L), rule.getFrequencyPeriod());
        assertEquals(Integer.valueOf(17), rule.getFrequencyLimit());
        assertNull(rule.getCountLimit());
    }

    @Test
    public void testParseRule_ConnectionFrequencyLimit_genericPeriod()
    {
        final RuleSetCreator creator = writeConfig("CLT user port=amqp connection-frequency-limit=17/P1dT1m17.7s");
        final Duration duration = Duration.parse("P1dT1m17.7s");

        final Rule rule = Iterables.getFirst(creator, null);
        assertNotNull(rule);
        assertEquals("user", rule.getIdentity());
        assertEquals("amqp", rule.getPort());
        assertFalse(rule.isUserBlocked());

        assertEquals(Collections.singletonMap(duration, 17), rule.getFrequencyLimits());
        assertEquals(duration, rule.getFrequencyPeriod());
        assertEquals(Integer.valueOf(17), rule.getFrequencyLimit());
        assertNull(rule.getCountLimit());
    }

    @Test
    public void testParseRule_ConnectionFrequencyLimit_noPeriod()
    {
        final RuleSetCreator creator = writeConfig("CLT user port=amqp connection-frequency-limit=17");

        final Rule rule = Iterables.getFirst(creator, null);
        assertNotNull(rule);
        assertEquals("user", rule.getIdentity());
        assertEquals("amqp", rule.getPort());
        assertFalse(rule.isUserBlocked());

        assertNull(rule.getFrequencyPeriod());
        assertEquals(Integer.valueOf(17), rule.getFrequencyLimit());
        assertNull(rule.getCountLimit());
    }

    @Test
    public void testParseRule_ConnectionLimit()
    {
        final RuleSetCreator creator = writeConfig("CLT user port=amqp connection_limit=10 connection-frequency-limit=20/m");

        final Rule rule = Iterables.getFirst(creator, null);
        assertNotNull(rule);
        assertEquals("user", rule.getIdentity());
        assertEquals("amqp", rule.getPort());
        assertFalse(rule.isUserBlocked());
        assertEquals(Integer.valueOf(10), rule.getCountLimit());
        assertEquals(Integer.valueOf(20), rule.getFrequencyLimit());
        assertEquals(Duration.ofMinutes(1L), rule.getFrequencyPeriod());
        assertEquals(Collections.singletonMap(Duration.ofMinutes(1L), 20), rule.getFrequencyLimits());
    }

    @Test
    public void testParseRule_ConnectionLimit2()
    {
        final RuleSetCreator creator = writeConfig("CLT\tuser\tport='amqp' connection_limit=\"10\" connection-frequency-limit='20/h'");

        final Rule rule = Iterables.getFirst(creator, null);
        assertNotNull(rule);
        assertEquals("user", rule.getIdentity());
        assertEquals("amqp", rule.getPort());
        assertFalse(rule.isUserBlocked());
        assertEquals(Integer.valueOf(10), rule.getCountLimit());
        assertEquals(Integer.valueOf(20), rule.getFrequencyLimit());
        assertEquals(Duration.ofHours(1L), rule.getFrequencyPeriod());
        assertEquals(Collections.singletonMap(Duration.ofHours(1L), 20), rule.getFrequencyLimits());
    }

    @Test
    public void testParseRule_Continuation()
    {
        final RuleSetCreator creator = writeConfig("CLT user \\ \n connectionLimit=10 connection-frequency-limit=20");

        final Rule rule = Iterables.getFirst(creator, null);
        assertNotNull(rule);
        assertEquals("user", rule.getIdentity());
        assertEquals(RulePredicates.ALL_PORTS, rule.getPort());
        assertFalse(rule.isUserBlocked());
        assertEquals(Integer.valueOf(10), rule.getCountLimit());
        assertEquals(Integer.valueOf(20), rule.getFrequencyLimit());
    }

    @Test
    public void testParseRule_Blocked()
    {
        final RuleSetCreator creator = writeConfig("CLT user block port=amqp");

        final Rule rule = Iterables.getFirst(creator, null);
        assertNotNull(rule);
        assertEquals("user", rule.getIdentity());
        assertEquals("amqp", rule.getPort());
        assertTrue(rule.isUserBlocked());
        assertEquals(Integer.valueOf(0), rule.getCountLimit());
        assertEquals(Integer.valueOf(0), rule.getFrequencyLimit());
        assertNotNull(rule.getFrequencyLimits());
        assertTrue(rule.getFrequencyLimits().isEmpty());
    }

    @Test
    public void testParseRule_DefaultPort()
    {
        final RuleSetCreator creator = writeConfig("CLT user connection_limit=10 connection-frequency-limit=20");

        final Rule rule = Iterables.getFirst(creator, null);
        assertNotNull(rule);
        assertEquals("user", rule.getIdentity());
        assertEquals(RulePredicates.ALL_PORTS, rule.getPort());
        assertFalse(rule.isUserBlocked());
        assertEquals(Integer.valueOf(10), rule.getCountLimit());
        assertEquals(Integer.valueOf(20), rule.getFrequencyLimit());
    }

    @Test
    public void testParseRule_Empty()
    {
        final RuleSetCreator creator = writeConfig("CLT user port=all");
        assertTrue(creator.isEmpty());
    }

    @Test
    public void testParseConfig()
    {
        final RuleSetCreator creator = writeConfig("CONFIG log-all=true default-frequency-period=200000");
        assertTrue(creator.isEmpty());
        assertTrue(creator.isLogAllMessages());
        assertEquals(Long.valueOf(200000L), creator.getDefaultFrequencyPeriod());
    }

    @Test
    public void testParseConfig_alternative()
    {
        final RuleSetCreator creator = writeConfig("CONFIG logAll=true defaultFrequencyPeriod=200000");
        assertTrue(creator.isEmpty());
        assertTrue(creator.isLogAllMessages());
        assertEquals(Long.valueOf(200000L), creator.getDefaultFrequencyPeriod());
    }

    @Test
    public void testParseConfig_Empty()
    {
        final RuleSetCreator creator = writeConfig("CONFIG log-all=false");
        assertTrue(creator.isEmpty());
        assertFalse(creator.isLogAllMessages());
        assertNull(creator.getDefaultFrequencyPeriod());
    }

    @Test
    public void testParseAclRule_ConnectionCountLimit()
    {
        final RuleSetCreator creator = writeConfig("ACL ALLOW-LOG user ACCESS VIRTUALHOST connection_limit=10");

        final Rule rule = Iterables.getFirst(creator, null);
        assertNotNull(rule);
        assertEquals("user", rule.getIdentity());
        assertEquals(RulePredicates.ALL_PORTS, rule.getPort());
        assertFalse(rule.isUserBlocked());
        assertEquals(Integer.valueOf(10), rule.getCountLimit());
        assertNull(rule.getFrequencyLimit());
        assertNotNull(rule.getFrequencyLimits());
        assertTrue(rule.getFrequencyLimits().isEmpty());
    }

    @Test
    public void testParseAclRule_ConnectionFrequencyLimit()
    {
        final RuleSetCreator creator = writeConfig("ACL ALLOW-LOG user ACCESS VIRTUALHOST connection_frequency_limit=60");

        final Rule rule = Iterables.getFirst(creator, null);
        assertNotNull(rule);
        assertEquals("user", rule.getIdentity());
        assertEquals(RulePredicates.ALL_PORTS, rule.getPort());
        assertFalse(rule.isUserBlocked());
        assertEquals(Integer.valueOf(60), rule.getFrequencyLimit());
        assertNull(rule.getCountLimit());
    }

    @Test
    public void testParseAclRule_ConnectionLimit()
    {
        final RuleSetCreator creator = writeConfig("ACL ALLOW-LOG user ACCESS VIRTUALHOST connection_limit=10 connection_frequency_limit=60");

        final Rule rule = Iterables.getFirst(creator, null);
        assertNotNull(rule);
        assertEquals("user", rule.getIdentity());
        assertEquals(RulePredicates.ALL_PORTS, rule.getPort());
        assertFalse(rule.isUserBlocked());
        assertEquals(Integer.valueOf(60), rule.getFrequencyLimit());
        assertEquals(Integer.valueOf(10), rule.getCountLimit());
    }

    @Test
    public void testParseAclRule_ConnectionLimit2()
    {
        final RuleSetCreator creator = writeConfig("27 ACL ALLOW-LOG user ACCESS VIRTUALHOST connection_limit=10 connection_frequency_limit=60");

        final Rule rule = Iterables.getFirst(creator, null);
        assertNotNull(rule);
        assertEquals("user", rule.getIdentity());
        assertEquals(RulePredicates.ALL_PORTS, rule.getPort());
        assertFalse(rule.isUserBlocked());
        assertEquals(Integer.valueOf(60), rule.getFrequencyLimit());
        assertEquals(Integer.valueOf(10), rule.getCountLimit());
    }

    @Test
    public void testParseAclRule_Empty()
    {
        final RuleSetCreator creator = writeConfig("ACL ALLOW-LOG user ACCESS VIRTUALHOST name=vhost");
        assertTrue(creator.isEmpty());
    }

    @Test
    public void testParseAclRule_Empty2()
    {
        final RuleSetCreator creator = writeConfig("ACL DENY user ACCESS");
        assertTrue(creator.isEmpty());
    }

    @Test
    public void testParse_Error_Continuation()
    {
        try
        {
            writeConfig("CLT user \\ connection_limit=10 connection_frequency_limit=60");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals(String.format(FileParser.PREMATURE_CONTINUATION, 1), e.getMessage());
        }
    }

    @Test
    public void testParse_Error_NotEnoughTokens()
    {
        try
        {
            writeConfig("CLT");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals(String.format(FileParser.NOT_ENOUGH_TOKENS, 1), e.getMessage());
        }
    }

    @Test
    public void testParse_Error_UnknownTokens()
    {
        try
        {
            writeConfig("CLT user connection_limit=10 connection_frequency_limit=60 name=vhost");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals(String.format(FileParser.UNKNOWN_CLT_PROPERTY_MSG, "name", 1), e.getMessage());
        }
    }

    @Test
    public void testParse_Error_OrderNumber()
    {
        try
        {
            writeConfig("7 CLT user connection_limit=10 connection_frequency_limit=60");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals(String.format(FileParser.NUMBER_NOT_ALLOWED, "CLT", 1), e.getMessage());
        }
    }

    @Test
    public void testParse_Error_NumberFormat()
    {
        try
        {
            writeConfig("CLT user connection_limit=10 connection_frequency_limit=xc");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals(String.format(FileParser.PARSE_TOKEN_FAILED, 1), e.getMessage());
        }
    }

    @Test
    public void testParse_Error_MissingEqualSign()
    {
        try
        {
            writeConfig("CLT user connection_limit=10 connection_frequency_limit,60");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals(String.format(FileParser.PROPERTY_NO_EQUALS_MSG, 1), e.getMessage());
        }
    }

    @Test
    public void testParse_Error_MissingValue()
    {
        try
        {
            writeConfig("CLT user connection_limit=10 connection_frequency_limit=");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals(String.format(FileParser.PROPERTY_NO_VALUE_MSG, 1), e.getMessage());
        }
    }

    @Test
    public void testParse_Error_KeyOnly()
    {
        try
        {
            writeConfig("CLT user connection_limit=10 connection_frequency_limit");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals(String.format(FileParser.PROPERTY_KEY_ONLY_MSG, 1), e.getMessage());
        }
    }

    @Test
    public void testParse_Error_UnknownToken()
    {
        try
        {
            writeConfig("GROUP group user");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals(String.format(FileParser.UNRECOGNISED_INITIAL_TOKEN, "GROUP", 1), e.getMessage());
        }
    }

    @Test
    public void testParse_Error_UnknownFile()
    {
        final String prefix = getClass().getSimpleName() + "." + getTestName();
        final Path connecionLimitFile;
        try
        {
            connecionLimitFile = Files.createTempFile(prefix, FILE_SUFFIX);
            Files.deleteIfExists(connecionLimitFile);
        }
        catch (IOException e)
        {
            throw new IllegalStateException("Failed to create a file: " + prefix + FILE_SUFFIX);
        }

        try
        {
            FileParser.parse(connecionLimitFile.toAbsolutePath().toString());
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void testParse_IOException() throws IOException
    {
        Reader reader = Mockito.mock(Reader.class);

        Mockito.doThrow(new IOException("exception")).when(reader).reset();
        Mockito.doThrow(new IOException("exception")).when(reader).ready();
        Mockito.doThrow(new IOException("exception")).when(reader).close();
        Mockito.doThrow(new IOException("exception")).when(reader).read(Mockito.any(CharBuffer.class));
        Mockito.doThrow(new IOException("exception")).when(reader).read(Mockito.any(char[].class));
        Mockito.doThrow(new IOException("exception")).when(reader).read(Mockito.any(char[].class), Mockito.anyInt(), Mockito.anyInt());
        Mockito.doThrow(new IOException("exception")).when(reader).mark(Mockito.anyInt());
        Mockito.doThrow(new IOException("exception")).when(reader).skip(Mockito.anyLong());

        try
        {
            new FileParser().readAndParse(reader);
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertNotNull(e.getMessage());
        }
    }

    private RuleSetCreator writeConfig(String... data)
    {
        final String prefix = getClass().getSimpleName() + "." + getTestName();
        final Path connectionLimitFile;
        try
        {
            connectionLimitFile = Files.createTempFile(prefix, FILE_SUFFIX);
            Files.deleteIfExists(connectionLimitFile);
        }
        catch (IOException e)
        {
            throw new IllegalStateException("Failed to create a file: " + prefix + FILE_SUFFIX);
        }

        try (BufferedWriter writer = Files.newBufferedWriter(connectionLimitFile, CHARSET))
        {
            for (String line : data)
            {
                writer.write(line);
                writer.newLine();
                writer.flush();
            }
        }
        catch (IOException x)
        {
            throw new IllegalStateException("Failed to write into the file " + connectionLimitFile.getFileName());
        }
        return FileParser.parse(connectionLimitFile.toAbsolutePath().toString());
    }
}