/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.qpid.server.security.access.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.nio.CharBuffer;

import org.junit.Test;
import org.mockito.Mockito;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.test.utils.UnitTestBase;

public class AclFileParserTest extends UnitTestBase
{
    private static final AclRulePredicates EMPTY = new AclRulePredicatesBuilder().build();

    private RuleSet writeACLConfig(String... aclData) throws Exception
    {
        final File acl = File.createTempFile(getClass().getName() + getTestName(), "acl");
        acl.deleteOnExit();

        // Write ACL file
        try (final PrintWriter aclWriter = new PrintWriter(new FileWriter(acl)))
        {
            for (final String line : aclData)
            {
                aclWriter.println(line);
            }
        }

        // Load ruleset
        return AclFileParser.parse(acl.toURI().toURL().toString(), mock(EventLoggerProvider.class));
    }

    @Test
    public void testEmptyRuleSetDefaults() throws Exception
    {
        final RuleSet ruleSet = writeACLConfig();
        assertEquals(0, ruleSet.getAllRules().size());
        assertEquals(Result.DENIED, ruleSet.getDefault());
    }

    @Test
    public void testACLFileSyntaxContinuation() throws Exception
    {
        try
        {
            writeACLConfig("ACL ALLOW ALL \\ ALL");
            fail("fail");
        }
        catch (IllegalConfigurationException ce)
        {
            assertEquals(String.format(AclFileParser.PREMATURE_CONTINUATION_MSG, 1), ce.getMessage());
        }
    }

    @Test
    public void testACLFileSyntaxTokens() throws Exception
    {
        try
        {
            writeACLConfig("ACL unparsed ALL ALL");
            fail("fail");
        }
        catch (IllegalConfigurationException ce)
        {
            assertEquals(String.format(AclFileParser.PARSE_TOKEN_FAILED_MSG, 1), ce.getMessage());
            assertTrue(ce.getCause() instanceof IllegalArgumentException);
            assertEquals("Not a valid permission: unparsed", ce.getCause().getMessage());
        }

        try
        {
            writeACLConfig("ACL DENY ALL unparsed");
            fail("fail");
        }
        catch (IllegalConfigurationException ce)
        {
            assertEquals(String.format(AclFileParser.PARSE_TOKEN_FAILED_MSG, 1), ce.getMessage());
            assertTrue(ce.getCause() instanceof IllegalArgumentException);
            assertEquals("Not a valid operation: unparsed", ce.getCause().getMessage());
        }

        try
        {
            writeACLConfig("ACL DENY ALL ALL unparsed");
            fail("fail");
        }
        catch (IllegalConfigurationException ce)
        {
            assertEquals(String.format(AclFileParser.PARSE_TOKEN_FAILED_MSG, 1), ce.getMessage());
            assertTrue(ce.getCause() instanceof IllegalArgumentException);
            assertEquals("Not a valid object type: unparsed", ce.getCause().getMessage());
        }
    }

    @Test
    public void testACLFileSyntaxNotEnoughACL() throws Exception
    {
        try
        {
            writeACLConfig("ACL ALLOW");
            fail("fail");
        }
        catch (IllegalConfigurationException ce)
        {
            assertEquals(String.format(AclFileParser.NOT_ENOUGH_ACL_MSG, 1), ce.getMessage());
        }
    }

    @Test
    public void testACLFileSyntaxNotEnoughConfig() throws Exception
    {
        try
        {
            writeACLConfig("CONFIG");
            fail("fail");
        }
        catch (IllegalConfigurationException ce)
        {
            assertEquals(String.format(AclFileParser.NOT_ENOUGH_TOKENS_MSG, 1), ce.getMessage());
        }

        try
        {
            writeACLConfig("CONFIG defaultdeny");
            fail("fail");
        }
        catch (IllegalConfigurationException ce)
        {
            assertEquals(String.format(AclFileParser.NOT_ENOUGH_CONFIG_MSG, 1), ce.getMessage());
        }
    }

    @Test
    public void testACLFileSyntaxOrderedConfig() throws Exception
    {
        try
        {
            writeACLConfig("3 CONFIG defaultdeny=true");
            fail("fail");
        }
        catch (IllegalConfigurationException ce)
        {
            assertEquals(String.format(AclFileParser.NUMBER_NOT_ALLOWED_MSG, "CONFIG", 1), ce.getMessage());
        }
    }

    @Test
    public void testACLFileSyntaxInvalidConfig() throws Exception
    {
        try
        {
            writeACLConfig("CONFIG default=false defaultdeny");
            fail("fail");
        }
        catch (IllegalConfigurationException ce)
        {
            assertEquals(String.format(AclFileParser.PROPERTY_KEY_ONLY_MSG, 1), ce.getMessage());
        }

        try
        {
            writeACLConfig("CONFIG default=false defaultdeny true");
            fail("fail");
        }
        catch (IllegalConfigurationException ce)
        {
            assertEquals(String.format(AclFileParser.PROPERTY_NO_EQUALS_MSG, 1), ce.getMessage());
        }

        try
        {
            writeACLConfig("CONFIG default=false defaultdeny=");
            fail("fail");
        }
        catch (IllegalConfigurationException ce)
        {
            assertEquals(String.format(AclFileParser.PROPERTY_NO_VALUE_MSG, 1), ce.getMessage());
        }
    }

    @Test
    public void testACLFileSyntaxNotEnough() throws Exception
    {
        try
        {
            writeACLConfig("INVALID");
            fail("fail");
        }
        catch (IllegalConfigurationException ce)
        {
            assertEquals(String.format(AclFileParser.NOT_ENOUGH_TOKENS_MSG, 1), ce.getMessage());
        }
    }

    @Test
    public void testACLFileSyntaxPropertyKeyOnly() throws Exception
    {
        try
        {
            writeACLConfig("ACL ALLOW adk CREATE QUEUE name");
            fail("fail");
        }
        catch (IllegalConfigurationException ce)
        {
            assertEquals(String.format(AclFileParser.PROPERTY_KEY_ONLY_MSG, 1), ce.getMessage());
        }
    }

    @Test
    public void testACLFileSyntaxPropertyNoEquals() throws Exception
    {
        try
        {
            writeACLConfig("ACL ALLOW adk CREATE QUEUE name test");
            fail("fail");
        }
        catch (IllegalConfigurationException ce)
        {
            assertEquals(String.format(AclFileParser.PROPERTY_NO_EQUALS_MSG, 1), ce.getMessage());
        }
    }

    @Test
    public void testACLFileSyntaxPropertyNoValue() throws Exception
    {
        try
        {
            writeACLConfig("ACL ALLOW adk CREATE QUEUE name =");
            fail("fail");
        }
        catch (IllegalConfigurationException ce)
        {
            assertEquals(String.format(AclFileParser.PROPERTY_NO_VALUE_MSG, 1), ce.getMessage());
        }
    }

    @Test
    public void testValidConfig() throws Exception
    {
        RuleSet ruleSet = writeACLConfig("CONFIG defaultdefer=true");
        assertEquals("Unexpected number of rules", 0, ruleSet.getAllRules().size());
        assertEquals("Unexpected default outcome", Result.DEFER, ruleSet.getDefault());

        ruleSet = writeACLConfig("CONFIG defaultdeny=true");
        assertEquals("Unexpected number of rules", 0, ruleSet.getAllRules().size());
        assertEquals("Unexpected default outcome", Result.DENIED, ruleSet.getDefault());

        ruleSet = writeACLConfig("CONFIG defaultallow=true");
        assertEquals("Unexpected number of rules", 0, ruleSet.getAllRules().size());
        assertEquals("Unexpected default outcome", Result.ALLOWED, ruleSet.getDefault());

        ruleSet = writeACLConfig("CONFIG defaultdefer=false defaultallow=true defaultdeny=false df=false");
        assertEquals("Unexpected number of rules", 0, ruleSet.getAllRules().size());
        assertEquals("Unexpected default outcome", Result.ALLOWED, ruleSet.getDefault());
    }

    /**
     * Tests interpretation of an acl rule with no object properties.
     */
    @Test
    public void testValidRule() throws Exception
    {
        final RuleSet rules = writeACLConfig("ACL DENY-LOG user1 ACCESS VIRTUALHOST");
        assertEquals(1, rules.getAllRules().size());

        final Rule rule = rules.getAllRules().get(0);
        assertEquals("Rule has unexpected identity", "user1", rule.getIdentity());
        assertEquals("Rule has unexpected operation", LegacyOperation.ACCESS, rule.getOperation());
        assertEquals("Rule has unexpected object type", ObjectType.VIRTUALHOST, rule.getObjectType());
        assertEquals("Rule has unexpected predicates", EMPTY, rule.getPredicates());
    }

    /**
     * Tests interpretation of an acl rule with object properties quoted in single quotes.
     */
    @Test
    public void testValidRuleWithSingleQuotedProperty() throws Exception
    {
        final RuleSet rules = writeACLConfig("ACL ALLOW all CREATE EXCHANGE name = 'value'");
        assertEquals(1, rules.getAllRules().size());

        final Rule rule = rules.getAllRules().get(0);
        assertEquals("Rule has unexpected identity", "all", rule.getIdentity());
        assertEquals("Rule has unexpected operation", LegacyOperation.CREATE, rule.getOperation());
        assertEquals("Rule has unexpected object type", ObjectType.EXCHANGE, rule.getObjectType());
        final AclRulePredicates expectedPredicates =
                new AclRulePredicatesBuilder()
                        .put(Property.NAME, "value")
                        .build();
        assertEquals("Rule has unexpected predicates", expectedPredicates, rule.getPredicates());
    }

    /**
     * Tests interpretation of an acl rule with object properties quoted in double quotes.
     */
    @Test
    public void testValidRuleWithDoubleQuotedProperty() throws Exception
    {
        final RuleSet rules = writeACLConfig("ACL ALLOW all CREATE EXCHANGE name = \"value\"");

        assertEquals(1, rules.getAllRules().size());
        final Rule rule = rules.getAllRules().get(0);
        assertEquals("Rule has unexpected identity", "all", rule.getIdentity());
        assertEquals("Rule has unexpected operation", LegacyOperation.CREATE, rule.getOperation());
        assertEquals("Rule has unexpected object type", ObjectType.EXCHANGE, rule.getObjectType());
        final AclRulePredicates expectedPredicates =
                new AclRulePredicatesBuilder()
                        .put(Property.NAME, "value")
                        .build();
        assertEquals("Rule has unexpected predicates", expectedPredicates, rule.getPredicates());
    }

    /**
     * Tests interpretation of an acl rule with many object properties.
     */
    @Test
    public void testValidRuleWithManyProperties() throws Exception
    {
        final RuleSet rules = writeACLConfig("ACL ALLOW admin DELETE QUEUE name=name1 owner = owner1");
        assertEquals(1, rules.getAllRules().size());

        final Rule rule = rules.getAllRules().get(0);
        assertEquals("Rule has unexpected identity", "admin", rule.getIdentity());
        assertEquals("Rule has unexpected operation", LegacyOperation.DELETE, rule.getOperation());
        assertEquals("Rule has unexpected object type", ObjectType.QUEUE, rule.getObjectType());
        final AclRulePredicates expectedPredicates =
                new AclRulePredicatesBuilder()
                        .put(Property.NAME, "name1")
                        .put(Property.OWNER, "owner1")
                        .build();
        assertEquals("Rule has unexpected operation", expectedPredicates, rule.getPredicates());
    }

    /**
     * Tests interpretation of an acl rule with object properties containing wildcards.  Values containing
     * hashes must be quoted otherwise they are interpreted as comments.
     */
    @Test
    public void testValidRuleWithWildcardProperties() throws Exception
    {
        final RuleSet rules = writeACLConfig("ACL ALLOW all CREATE EXCHANGE routingKey = 'news.#'",
                                             "ACL ALLOW all CREATE EXCHANGE routingKey = 'news.co.#'",
                                             "ACL ALLOW all CREATE EXCHANGE routingKey = *.co.medellin");
        assertEquals(3, rules.getAllRules().size());

        final Rule rule1 = rules.getAllRules().get(0);
        assertEquals("Rule has unexpected identity", "all", rule1.getIdentity());
        assertEquals("Rule has unexpected operation", LegacyOperation.CREATE, rule1.getOperation());
        assertEquals("Rule has object type", ObjectType.EXCHANGE, rule1.getObjectType());
        final AclRulePredicates expectedPredicates1 =
                new AclRulePredicatesBuilder()
                        .put(Property.ROUTING_KEY, "news.#")
                        .build();
        assertEquals("Rule has unexpected predicates", expectedPredicates1, rule1.getPredicates());

        final Rule rule2 = rules.getAllRules().get(1);
        final AclRulePredicates expectedPredicates2 =
                new AclRulePredicatesBuilder()
                        .put(Property.ROUTING_KEY, "news.co.#")
                        .build();
        assertEquals("Rule has unexpected predicates", expectedPredicates2, rule2.getPredicates());

        final Rule rule3 = rules.getAllRules().get(2);
        final AclRulePredicates expectedPredicates3 =
                new AclRulePredicatesBuilder()
                        .put(Property.ROUTING_KEY, "*.co.medellin")
                        .build();
        assertEquals("Rule has unexpected predicates", expectedPredicates3, rule3.getPredicates());
    }

    @Test
    public void testOrderedValidRule() throws Exception
    {
        final RuleSet rules = writeACLConfig("5 ACL DENY all CREATE EXCHANGE",
                                             "3 ACL ALLOW all CREATE EXCHANGE routingKey = 'news.co.#'",
                                             "1 ACL ALLOW all CREATE EXCHANGE routingKey = *.co.medellin");
        assertEquals(3, rules.getAllRules().size());

        final Rule rule1 = rules.getAllRules().get(0);
        final AclRulePredicates expectedPredicates1 =
                new AclRulePredicatesBuilder()
                        .put(Property.ROUTING_KEY, "*.co.medellin")
                        .build();
        assertEquals("Rule has unexpected predicates", expectedPredicates1, rule1.getPredicates());

        final Rule rule3 = rules.getAllRules().get(1);
        final AclRulePredicates expectedPredicates3 =
                new AclRulePredicatesBuilder()
                        .put(Property.ROUTING_KEY, "news.co.#")
                        .build();
        assertEquals("Rule has unexpected predicates", expectedPredicates3, rule3.getPredicates());

        final Rule rule5 = rules.getAllRules().get(2);
        assertEquals("Rule has unexpected identity", "all", rule5.getIdentity());
        assertEquals("Rule has unexpected operation", LegacyOperation.CREATE, rule5.getOperation());
        assertEquals("Rule has unexpected object type", ObjectType.EXCHANGE, rule5.getObjectType());
        final AclRulePredicates expectedPredicates5 = new AclRulePredicatesBuilder().build();
        assertEquals("Rule has unexpected predicates", expectedPredicates5, rule5.getPredicates());
    }

    @Test
    public void testOrderedInValidRule() throws Exception
    {
        try
        {
            writeACLConfig("5 ACL DENY all CREATE EXCHANGE",
                           "3 ACL ALLOW all CREATE EXCHANGE routingKey = 'news.co.#'",
                           "5 ACL ALLOW all CREATE EXCHANGE routingKey = *.co.medellin");
            fail("fail");
        }
        catch (IllegalConfigurationException ce)
        {
            assertEquals(String.format(AclFileParser.BAD_ACL_RULE_NUMBER_MSG, 3), ce.getMessage());
        }
    }

    @Test
    public void testShortValidRule() throws Exception
    {
        final RuleSet rules = writeACLConfig("ACL DENY user UPDATE");
        assertEquals(1, rules.getAllRules().size());
        validateRule(rules, "user", LegacyOperation.UPDATE, ObjectType.ALL, EMPTY);
    }

    /**
     * Tests that rules are case insignificant.
     */
    @Test
    public void testMixedCaseRuleInterpretation() throws Exception
    {
        final RuleSet rules = writeACLConfig("AcL deny-LOG User1 BiND Exchange Name=AmQ.dIrect");
        assertEquals(1, rules.getAllRules().size());

        final Rule rule = rules.getAllRules().get(0);
        assertEquals("Rule has unexpected identity", "User1", rule.getIdentity());
        assertEquals("Rule has unexpected operation", LegacyOperation.BIND, rule.getOperation());
        assertEquals("Rule has unexpected object type", ObjectType.EXCHANGE, rule.getObjectType());
        final AclRulePredicates expectedPredicates =
                new AclRulePredicatesBuilder()
                        .put(Property.NAME, "AmQ.dIrect")
                        .build();
        assertEquals("Rule has unexpected predicates", expectedPredicates, rule.getPredicates());
    }

    /**
     * Tests whitespace is supported. Note that currently the Java implementation permits comments to
     * be introduced anywhere in the ACL, whereas the C++ supports only whitespace at the beginning of
     * of line.
     */
    @Test
    public void testCommentsSupported() throws Exception
    {
        final RuleSet rules = writeACLConfig("#Comment",
                                             "ACL DENY-LOG user1 ACCESS VIRTUALHOST # another comment",
                                             "  # final comment with leading whitespace");
        assertEquals(1, rules.getAllRules().size());

        final Rule rule = rules.getAllRules().get(0);
        assertEquals("Rule has unexpected identity", "user1", rule.getIdentity());
        assertEquals("Rule has unexpected operation", LegacyOperation.ACCESS, rule.getOperation());
        assertEquals("Rule has unexpected object type", ObjectType.VIRTUALHOST, rule.getObjectType());
        assertEquals("Rule has unexpected predicates", EMPTY, rule.getPredicates());
    }

    /**
     * Tests interpretation of an acl rule using mixtures of tabs/spaces as token separators.
     */
    @Test
    public void testWhitespace() throws Exception
    {
        final RuleSet rules = writeACLConfig("ACL\tDENY-LOG\t\t user1\t \tACCESS VIRTUALHOST");
        assertEquals(1, rules.getAllRules().size());

        final Rule rule = rules.getAllRules().get(0);
        assertEquals("Rule has unexpected identity", "user1", rule.getIdentity());
        assertEquals("Rule has unexpected operation", LegacyOperation.ACCESS, rule.getOperation());
        assertEquals("Rule has unexpected object type", ObjectType.VIRTUALHOST, rule.getObjectType());
        assertEquals("Rule has unexpected predicates", EMPTY, rule.getPredicates());
    }

    @Test
    public void testWhitespace2() throws Exception
    {
        final RuleSet rules = writeACLConfig("ACL\u000B DENY-LOG\t\t user1\t \tACCESS VIRTUALHOST\u001E");
        assertEquals(1, rules.getAllRules().size());

        final Rule rule = rules.getAllRules().get(0);
        assertEquals("Rule has unexpected identity", "user1", rule.getIdentity());
        assertEquals("Rule has unexpected operation", LegacyOperation.ACCESS, rule.getOperation());
        assertEquals("Rule has unexpected object type", ObjectType.VIRTUALHOST, rule.getObjectType());
        assertEquals("Rule has unexpected predicates", EMPTY, rule.getPredicates());
    }

    /**
     * Tests interpretation of an acl utilising line continuation.
     */
    @Test
    public void testLineContinuation() throws Exception
    {
        final RuleSet rules = writeACLConfig("ACL DENY-LOG user1 \\",
                                             "ACCESS VIRTUALHOST");
        assertEquals(1, rules.getAllRules().size());

        final Rule rule = rules.getAllRules().get(0);
        assertEquals("Rule has unexpected identity", "user1", rule.getIdentity());
        assertEquals("Rule has unexpected operation", LegacyOperation.ACCESS, rule.getOperation());
        assertEquals("Rule has unexpected object type", ObjectType.VIRTUALHOST, rule.getObjectType());
        assertEquals("Rule has unexpected predicates", EMPTY, rule.getPredicates());
    }

    @Test
    public void testUserRuleParsing() throws Exception
    {
        final AclRulePredicates predicates =
                new AclRulePredicatesBuilder()
                        .put(Property.NAME, "otherUser")
                        .build();

        validateRule(writeACLConfig("ACL ALLOW user1 CREATE USER"),
                           "user1", LegacyOperation.CREATE, ObjectType.USER, EMPTY);
        validateRule(writeACLConfig("ACL ALLOW user1 CREATE USER name=\"otherUser\""),
                           "user1", LegacyOperation.CREATE, ObjectType.USER, predicates);

        validateRule(writeACLConfig("ACL ALLOW user1 DELETE USER"),
                           "user1", LegacyOperation.DELETE, ObjectType.USER, EMPTY);
        validateRule(writeACLConfig("ACL ALLOW user1 DELETE USER name=\"otherUser\""),
                           "user1", LegacyOperation.DELETE, ObjectType.USER, predicates);

        validateRule(writeACLConfig("ACL ALLOW user1 UPDATE USER"),
                           "user1", LegacyOperation.UPDATE, ObjectType.USER, EMPTY);
        validateRule(writeACLConfig("ACL ALLOW user1 UPDATE USER name=\"otherUser\""),
                           "user1", LegacyOperation.UPDATE, ObjectType.USER, predicates);

        validateRule(writeACLConfig("ACL ALLOW user1 ALL USER"),
                           "user1", LegacyOperation.ALL, ObjectType.USER, EMPTY);
        validateRule(writeACLConfig("ACL ALLOW user1 ALL USER name=\"otherUser\""),
                           "user1", LegacyOperation.ALL, ObjectType.USER, predicates);
    }

    @Test
    public void testGroupRuleParsing() throws Exception
    {
        final AclRulePredicates predicates =
                new AclRulePredicatesBuilder()
                        .put(Property.NAME, "groupName")
                        .build();

        validateRule(writeACLConfig("ACL ALLOW user1 CREATE GROUP"),
                           "user1", LegacyOperation.CREATE, ObjectType.GROUP, EMPTY);
        validateRule(writeACLConfig("ACL ALLOW user1 CREATE GROUP name=\"groupName\""),
                           "user1", LegacyOperation.CREATE, ObjectType.GROUP, predicates);

        validateRule(writeACLConfig("ACL ALLOW user1 DELETE GROUP"),
                           "user1", LegacyOperation.DELETE, ObjectType.GROUP, EMPTY);
        validateRule(writeACLConfig("ACL ALLOW user1 DELETE GROUP name=\"groupName\""),
                           "user1", LegacyOperation.DELETE, ObjectType.GROUP, predicates);

        validateRule(writeACLConfig("ACL ALLOW user1 UPDATE GROUP"),
                           "user1", LegacyOperation.UPDATE, ObjectType.GROUP, EMPTY);
        validateRule(writeACLConfig("ACL ALLOW user1 UPDATE GROUP name=\"groupName\""),
                           "user1", LegacyOperation.UPDATE, ObjectType.GROUP, predicates);

        validateRule(writeACLConfig("ACL ALLOW user1 ALL GROUP"),
                           "user1", LegacyOperation.ALL, ObjectType.GROUP, EMPTY);
        validateRule(writeACLConfig("ACL ALLOW user1 ALL GROUP name=\"groupName\""),
                           "user1", LegacyOperation.ALL, ObjectType.GROUP, predicates);
    }

    /**
     * explicitly test for exception indicating that this functionality has been moved to Group Providers
     */
    @Test
    public void testGroupDefinitionThrowsException() throws Exception
    {
        try
        {
            writeACLConfig("GROUP group1 bob alice");
            fail("Expected exception not thrown");
        }
        catch (IllegalConfigurationException e)
        {
            assertTrue(e.getMessage().contains("GROUP keyword not supported"));
        }
    }

    @Test
    public void testUnknownDefinitionThrowsException() throws Exception
    {
        try
        {
            writeACLConfig("Unknown group1 bob alice");
            fail("Expected exception not thrown");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals(String.format(AclFileParser.UNRECOGNISED_INITIAL_MSG, "Unknown", 1), e.getMessage());
        }
    }

    @Test
    public void testManagementRuleParsing() throws Exception
    {
        validateRule(writeACLConfig("ACL ALLOW user1 ALL MANAGEMENT"),
                           "user1", LegacyOperation.ALL, ObjectType.MANAGEMENT, EMPTY);

        validateRule(writeACLConfig("ACL ALLOW user1 ACCESS MANAGEMENT"),
                           "user1", LegacyOperation.ACCESS, ObjectType.MANAGEMENT, EMPTY);
    }

    @Test
    public void testBrokerRuleParsing() throws Exception
    {
        validateRule(writeACLConfig("ACL ALLOW user1 CONFIGURE BROKER"),
                           "user1",
                                    LegacyOperation.CONFIGURE,
                                    ObjectType.BROKER,
                                    EMPTY);
        validateRule(writeACLConfig("ACL ALLOW user1 ALL BROKER"),
                           "user1",
                                    LegacyOperation.ALL,
                                    ObjectType.BROKER,
                                    EMPTY);
    }

    @Test
    public void testReaderIOException() throws IOException
    {
        Reader reader = mock(Reader.class);
        doReturn(true).when(reader).ready();
        doReturn(1L).when(reader).skip(Mockito.anyLong());
        doThrow(IOException.class).when(reader).read();
        doThrow(IOException.class).when(reader).read(Mockito.any(CharBuffer.class));
        doThrow(IOException.class).when(reader).read(Mockito.any(char[].class));
        doThrow(IOException.class).when(reader)
                                  .read(Mockito.any(char[].class), Mockito.anyInt(), Mockito.anyInt());
        try
        {
            new AclFileParser().readAndParse(reader);
            fail("Expected exception not thrown");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals(AclFileParser.CANNOT_LOAD_MSG, e.getMessage());
        }
    }

    @Test
    public void testReaderRuntimeException() throws IOException
    {
        Reader reader = mock(Reader.class);
        doReturn(true).when(reader).ready();
        doReturn(1L).when(reader).skip(Mockito.anyLong());
        doThrow(RuntimeException.class).when(reader).read();
        doThrow(RuntimeException.class).when(reader).read(Mockito.any(CharBuffer.class));
        doThrow(RuntimeException.class).when(reader).read(Mockito.any(char[].class));
        doThrow(RuntimeException.class).when(reader)
                                       .read(Mockito.any(char[].class), Mockito.anyInt(), Mockito.anyInt());
        try
        {
            new AclFileParser().readAndParse(reader);
            fail("Expected exception not thrown");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals(String.format(AclFileParser.PARSE_TOKEN_FAILED_MSG, 0), e.getMessage());
        }
    }

    private void validateRule(RuleSet rules,
                              String username,
                              LegacyOperation operation,
                              ObjectType objectType,
                              AclRulePredicates predicates)
    {
        assertEquals(1, rules.getAllRules().size());
        final Rule rule = rules.getAllRules().get(0);
        assertEquals("Rule has unexpected identity", username, rule.getIdentity());
        assertEquals("Rule has unexpected operation", operation, rule.getOperation());
        assertEquals("Rule has unexpected object type", objectType, rule.getObjectType());
        assertEquals("Rule has unexpected predicates", predicates, rule.getPredicates());
    }

    @Test
    public void testConnectionLimitParsing() throws Exception
    {
        validateRule(writeACLConfig("ACL ALLOW all ACCESS VIRTUALHOST connection_limit=10 connection_frequency_limit=12"),
                Rule.ALL,
                LegacyOperation.ACCESS,
                ObjectType.VIRTUALHOST,
                EMPTY);
    }
}
