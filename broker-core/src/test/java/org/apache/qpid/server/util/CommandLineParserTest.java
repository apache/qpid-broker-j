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

package org.apache.qpid.server.util;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

/**
 * Unit tests the {@link CommandLineParser} class.
 * <p>
 * <p><table id="crc"><caption>CRC Card</caption>
 * <tr><th> Responsibilities <th> Collaborations
 * <tr><td> Check that parsing a single flag works ok.
 * <tr><td> Check that parsing multiple flags condensed together works ok.
 * <tr><td> Check that parsing an option with a space between it and its argument works ok.
 * <tr><td> Check that parsing an option with no space between it and its argument works ok.
 * <tr><td> Check that parsing an option with specific argument format works ok.
 * <tr><td> Check that parsing an option with specific argument format fails on bad argument.
 * <tr><td> Check that parsing a flag condensed together with an option fails.
 * <tr><td> Check that parsing a free argument works ok.
 * <tr><td> Check that parsing a free argument with specific format works ok.
 * <tr><td> Check that parsing a free argument with specific format fails on bad argument.
 * <tr><td> Check that parsing a mandatory option works ok.
 * <tr><td> Check that parsing a mandatory free argument works ok.
 * <tr><td> Check that parsing a mandatory option fails when no option is set.
 * <tr><td> Check that parsing a mandatory free argument fails when no argument is specified.
 * <tr><td> Check that parsing an unknown option works when unknowns not errors.
 * <tr><td> Check that parsing an unknown flag fails when unknowns are to be reported as errors.
 * <tr><td> Check that parsing an unknown option fails when unknowns are to be reported as errors.
 * <tr><td> Check that get errors returns a string on errors.
 * <tr><td> Check that get errors returns an empty string on no errors.
 * <tr><td> Check that get usage returns a string.
 * <tr><td> Check that get options in force returns an empty string before parsing.
 * <tr><td> Check that get options in force return a non-empty string after parsing.
 * </table>
 */
public class CommandLineParserTest extends UnitTestBase
{
    /** Check that get errors returns an empty string on no errors. */
    @Test
    public void testGetErrorsReturnsEmptyStringOnNoErrors()
    {
        // Create a command line parser for some flags and options.
        final CommandLineParser parser = new CommandLineParser(new String[][]
        {
                { "t1", "Test Flag 1." },
                { "t2", "Test Option 2.", "test" },
                { "t3", "Test Option 3.", "test", "true" },
                { "t4", "Test Option 4.", "test", null, "^test$" }
        });

        // Do some legal parsing.
        parser.parseCommandLine(new String[] { "-t1", "-t2test", "-t3test", "-t4test" });

        // Check that the get errors message returns an empty string.
        assertEquals("", parser.getErrors(), "The errors method did not return an empty string.");
    }

    /** Check that get errors returns a string on errors. */
    @Test
    public void testGetErrorsReturnsStringOnErrors()
    {
        // Create a command line parser for some flags and options.
        final CommandLineParser parser = new CommandLineParser(new String[][]
        {
                { "t1", "Test Flag 1." },
                { "t2", "Test Option 2.", "test" },
                { "t3", "Test Option 3.", "test", "true" },
                { "t4", "Test Option 4.", "test", null, "^test$" }
        });

        assertThrows(IllegalArgumentException.class,
                () -> parser.parseCommandLine(new String[] { "-t1", "-t1t2test", "-t4fail" }));

        // Check that the get errors message returns a string.
        final boolean condition = !((parser.getErrors() == null) || "".equals(parser.getErrors()));
        assertTrue(condition, "The errors method returned an empty string.");
    }

    /** Check that get options in force returns an empty string before parsing. */
    @Test
    public void testGetOptionsInForceReturnsEmptyStringBeforeParsing()
    {
        // Create a command line parser for some flags and options.
        final CommandLineParser parser = new CommandLineParser(new String[][]
        {
                { "t1", "Test Flag 1." },
                { "t2", "Test Option 2.", "test" },
                { "t3", "Test Option 3.", "test", "true" },
                { "t4", "Test Option 4.", "test", null, "^test$" }
        });

        // Check that the options in force method returns an empty string.
        assertEquals("", parser.getOptionsInForce(), "The options in force method did not return an empty string.");
    }

    /** Check that get options in force return a non-empty string after parsing. */
    @Test
    public void testGetOptionsInForceReturnsNonEmptyStringAfterParsing()
    {
        // Create a command line parser for some flags and options.
        final CommandLineParser parser = new CommandLineParser(new String[][]
        {
                { "t1", "Test Flag 1." },
                { "t2", "Test Option 2.", "test" },
                { "t3", "Test Option 3.", "test", "true" },
                { "t4", "Test Option 4.", "test", null, "^test$" }
        });

        // Do some parsing.
        parser.parseCommandLine(new String[] { "-t1", "-t2test", "-t3test", "-t4test" });

        // Check that the options in force method returns a string.
        final boolean condition = !((parser.getOptionsInForce() == null) || "".equals(parser.getOptionsInForce()));
        assertTrue(condition, "The options in force method did not return a non empty string.");
    }

    /** Check that get usage returns a string. */
    @Test
    public void testGetUsageReturnsString()
    {
        // Create a command line parser for some flags and options.
        final CommandLineParser parser = new CommandLineParser(new String[][]
        {
                { "t1", "Test Flag 1." },
                { "t2", "Test Option 2.", "test" },
                { "t3", "Test Option 3.", "test", "true" },
                { "t4", "Test Option 4.", "test", null, "^test$" }
        });

        // Check that the usage method returns a string.
        final boolean condition = !((parser.getUsage() == null) || "".equals(parser.getUsage()));
        assertTrue(condition, "The usage method did not return a non empty string.");
    }

    /** Check that parsing multiple flags condensed together works ok. */
    @Test
    public void testParseCondensedFlagsOk()
    {
        // Create a command line parser for multiple flags.
        final CommandLineParser parser = new CommandLineParser(new String[][]
        {
                { "t1", "Test Flag 1." },
                { "t2", "Test Flag 2." },
                { "t3", "Test Flag 3." }
        });

        // Parse a command line with the flags set and condensed together.
        final Properties testProps = parser.parseCommandLine(new String[] { "-t1t2t3" });

        // Check that the flags were set in the parsed properties.
        assertEquals("true", testProps.get("t1"), "The t1 flag was not \"true\", it was: " + testProps.get("t1"));
        assertEquals("true", testProps.get("t2"), "The t2 flag was not \"true\", it was: " + testProps.get("t2"));
        assertEquals("true", testProps.get("t3"), "The t3 flag was not \"true\", it was: " + testProps.get("t3"));
    }

    /** Check that parsing a flag condensed together with an option fails. */
    @Test
    public void testParseFlagCondensedWithOptionFails()
    {
        // Create a command line parser for a flag and an option.
        final CommandLineParser parser = new CommandLineParser(new String[][]
        {
                { "t1", "Test Flag 1." },
                { "t2", "Test Option 2.", "test" }
        });

        assertThrows(IllegalArgumentException.class,
                () -> parser.parseCommandLine(new String[] { "-t1t2" }),
                "IllegalArgumentException not thrown when a flag and option are condensed together.");
    }

    /** Check that parsing a free argument with specific format fails on bad argument. */
    @Test
    public void testParseFormattedFreeArgumentFailsBadArgument()
    {
        // Create a command line parser for a formatted free argument.
        final CommandLineParser parser = new CommandLineParser(new String[][]
        {
                { "1", "Test Free Argument.", "test", null, "^test$" }
        });

        // Check that the parser signals an error for a badly formatted argument.
        assertThrows(IllegalArgumentException.class,
                () -> parser.parseCommandLine(new String[] { "fail" }),
                "IllegalArgumentException not thrown when a badly formatted argument was set.");
    }

    /** Check that parsing a free argument with specific format works ok. */
    @Test
    public void testParseFormattedFreeArgumentOk()
    {
        // Create a command line parser for a formatted free argument.
        final CommandLineParser parser = new CommandLineParser(new String[][]
        {
                { "1", "Test Free Argument.", "test", null, "^test$" }
        });

        // Parse a command line with this argument set correctly.
        final Properties testProps = parser.parseCommandLine(new String[] { "test" });

        // Check that the resultant properties contains the correctly parsed option.
        assertEquals("test", testProps.get("1"),
                "The first free argument was not equal to \"test\" but was: " + testProps.get("1"));
    }

    /** Check that parsing an option with specific argument format fails on bad argument. */
    @Test
    public void testParseFormattedOptionArgumentFailsBadArgument()
    {
        // Create a command line parser for a formatted option.
        final CommandLineParser parser = new CommandLineParser(new String[][]
        {
                { "t", "Test Option.", "test", null, "^test$" }
        });

        // Check that the parser signals an error for a badly formatted argument
        assertThrows(IllegalArgumentException.class,
                () -> parser.parseCommandLine(new String[] { "-t", "fail" }),
                "IllegalArgumentException not thrown when a badly formatted argument was set.");
    }

    /** Check that parsing an option with specific argument format works ok. */
    @Test
    public void testParseFormattedOptionArgumentOk()
    {
        // Create a command line parser for a formatted option.
        final CommandLineParser parser = new CommandLineParser(new String[][]
        {
                { "t", "Test Option.", "test", null, "^test$" }
        });

        // Parse a command line with this option set correctly.
        final Properties testProps = parser.parseCommandLine(new String[] { "-t", "test" });

        // Check that the resultant properties contains the correctly parsed option.
        assertEquals("test", testProps.get("t"),
                "The test option was not equal to \"test\" but was: " + testProps.get("t"));
    }

    /** Check that parsing a free argument works ok. */
    @Test
    public void testParseFreeArgumentOk()
    {
        // Create a command line parser for a free argument.
        final CommandLineParser parser = new CommandLineParser(new String[][]
        {
                { "1", "Test Free Argument.", "test" }
        });

        // Parse a command line with this argument set.
        final Properties testProps = parser.parseCommandLine(new String[] { "test" });

        // Check that the resultant properties contains the correctly parsed option.
        assertEquals("test", testProps.get("1"),
                "The first free argument was not equal to \"test\" but was: " + testProps.get("1"));
    }

    /** Check that parsing a mandatory option works ok. */
    @Test
    public void testParseMandatoryOptionOk()
    {
        // Create a command line parser for a mandatory option.
        final CommandLineParser parser = new CommandLineParser(new String[][]
        {
                { "t", "Test Option.", "test", "true" }
        });

        // Parse a command line with this option set correctly.
        final Properties testProps = parser.parseCommandLine(new String[] { "-t", "test" });

        // Check that the resultant properties contains the correctly parsed option.
        assertEquals("test", testProps.get("t"),
                "The test option was not equal to \"test\" but was: " + testProps.get("t"));
    }

    /** Check that parsing a mandatory free argument works ok. */
    @Test
    public void testParseMandatoryFreeArgumentOk()
    {
        // Create a command line parser for a mandatory free argument.
        final CommandLineParser parser = new CommandLineParser(new String[][]
        {
                { "1", "Test Option.", "test", "true" }
        });

        // Parse a command line with this argument set.
        final Properties testProps = parser.parseCommandLine(new String[] { "test" });

        // Check that the resultant properties contains the correctly parsed option.
        assertEquals("test", testProps.get("1"),
                "The first free argument was not equal to \"test\" but was: " + testProps.get("1"));
    }

    /** Check that parsing a mandatory free argument fails when no argument is specified. */
    @Test
    public void testParseManadatoryFreeArgumentFailsNoArgument()
    {
        // Create a command line parser for a mandatory free argument.
        final CommandLineParser parser = new CommandLineParser(new String[][]
        {
                { "1", "Test Option.", "test", "true" }
        });

        // Check that parsing fails when this mandatory free argument is missing
        assertThrows(IllegalArgumentException.class,
                () -> parser.parseCommandLine(new String[] {}),
                "IllegalArgumentException not thrown for a missing mandatory option.");
    }

    /** Check that parsing a mandatory option fails when no option is set. */
    @Test
    public void testParseMandatoryFailsNoOption()
    {
        // Create a command line parser for a mandatory option.
        final CommandLineParser parser = new CommandLineParser(new String[][]
        {
                { "t", "Test Option.", "test", "true" }
        });

        // Check that parsing fails when this mandatory option is missing
        assertThrows(IllegalArgumentException.class,
                () -> parser.parseCommandLine(new String[] {}),
                "IllegalArgumentException not thrown for a missing mandatory option.");
    }

    /** Check that parsing an option with no space between it and its argument works ok. */
    @Test
    public void testParseOptionWithNoSpaceOk()
    {
        // Create a command line parser for an option.
        final CommandLineParser parser = new CommandLineParser(new String[][]
        {
                { "t", "Test Option.", "test" }
        });

        // Parse a command line with this option set with no space.
        final Properties testProps = parser.parseCommandLine(new String[] { "-ttest" });

        // Check that the resultant properties contains the correctly parsed option.
        assertEquals("test", testProps.get("t"),
            "The test option was not equal to \"test\" but was: " + testProps.get("t"));
    }

    /** Check that parsing an option with a space between it and its argument works ok. */
    @Test
    public void testParseOptionWithSpaceOk()
    {
        // Create a command line parser for an option.
        final CommandLineParser parser = new CommandLineParser(new String[][]
        {
                { "t", "Test Option.", "test" }
        });

        // Parse a command line with this option set with a space.
        final Properties testProps = parser.parseCommandLine(new String[] { "-t", "test" });

        // Check that the resultant properties contains the correctly parsed option.
        assertEquals("test", testProps.get("t"),
                "The test option was not equal to \"test\" but was: " + testProps.get("t"));
    }

    /** Check that parsing a single flag works ok. */
    @Test
    public void testParseSingleFlagOk()
    {
        // Create a command line parser for a single flag.
        final CommandLineParser parser = new CommandLineParser(new String[][]
        {
                { "t", "Test Flag." }
        });

        // Parse a command line with the single flag set.
        Properties testProps = parser.parseCommandLine(new String[] { "-t" });

        // Check that the flag is set in the parsed properties.
        assertEquals("true", testProps.get("t"), "The t flag was not \"true\", it was: " + testProps.get("t"));

        // Reset the parser.
        parser.reset();

        // Parse a command line with the single flag not set.
        testProps = parser.parseCommandLine(new String[] {});

        // Check that the flag is cleared in the parsed properties.
        assertEquals("false", testProps.get("t"), "The t flag was not \"false\", it was: " + testProps.get("t"));
    }

    /** Check that parsing an unknown option works when unknowns not errors. */
    @Test
    public void testParseUnknownOptionOk()
    {
        // Create a command line parser for no flags or options
        final CommandLineParser parser = new CommandLineParser(new String[][] {});

        // Check that parsing does not fail on an unknown flag.
        assertDoesNotThrow(() -> parser.parseCommandLine(new String[] { "-t" }),
                "The parser threw an IllegalArgumentException on an unknown flag when errors on unkowns is off.");
    }

    /** Check that parsing an unknown flag fails when unknowns are to be reported as errors. */
    @Test
    public void testParseUnknownFlagFailsWhenUnknownsAreErrors()
    {
        // Create a command line parser for no flags or options
        final CommandLineParser parser = new CommandLineParser(new String[][] {});

        // Turn on fail on unknowns mode.
        parser.setErrorsOnUnknowns(true);

        // Check that parsing fails on an unknown flag
        assertThrows(IllegalArgumentException.class,
                () -> parser.parseCommandLine(new String[] { "-t" }),
                "IllegalArgumentException not thrown for an unknown flag when errors on unknowns mode is on.");
    }

    /** Check that parsing an unknown option fails when unknowns are to be reported as errors. */
    @Test
    public void testParseUnknownOptionFailsWhenUnknownsAreErrors()
    {
        // Create a command line parser for no flags or options
        final CommandLineParser parser = new CommandLineParser(new String[][] {});

        // Turn on fail on unknowns mode.
        parser.setErrorsOnUnknowns(true);

        // Check that parsing fails on an unknown flag
        assertThrows(IllegalArgumentException.class,
                () -> parser.parseCommandLine(new String[] { "-t", "test" }),
                "IllegalArgumentException not thrown for an unknown option when errors on unknowns mode is on.");
    }
}
