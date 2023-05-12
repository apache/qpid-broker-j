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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.nio.CharBuffer;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.test.utils.UnitTestBase;

class AclFileParserTest extends UnitTestBase
{
    private static final AclRulePredicates EMPTY = new AclRulePredicatesBuilder().build();

    private RuleSet writeACLConfig(final String... aclData)
    {
        try
        {
            final File acl = File.createTempFile(getClass().getName() + getTestName(), "acl");
            acl.deleteOnExit();

            // Write ACL file
            try (final PrintWriter aclWriter = new PrintWriter(new FileWriter(acl)))
            {
                Arrays.stream(aclData).forEach(aclWriter::println);
            }

            // Load ruleset
            return AclFileParser.parse(acl.toURI().toURL().toString(), mock(EventLoggerProvider.class));
        }
        catch (IllegalConfigurationException e)
        {
            throw e;
        }
        catch (Exception e)
        {
           throw new RuntimeException(e);
        }
    }

    @Test
    void emptyRuleSetDefaults()
    {
        final RuleSet ruleSet = writeACLConfig();
        assertEquals(0, ruleSet.size());
        assertEquals(Result.DENIED, ruleSet.getDefault());
    }

    @Test
    void ACLFileSyntaxContinuation()
    {
        final IllegalConfigurationException thrown = assertThrows(IllegalConfigurationException.class,
                () -> writeACLConfig("ACL ALLOW ALL \\ ALL"),
                "fail");
        assertEquals(String.format(AclFileParser.PREMATURE_CONTINUATION_MSG, 1), thrown.getMessage());
    }

    @ParameterizedTest
    @CsvSource(
    {
            "ACL unparsed ALL ALL, " + AclFileParser.PARSE_TOKEN_FAILED_MSG + ", Not a valid permission: unparsed",
            "ACL DENY ALL unparsed, " + AclFileParser.PARSE_TOKEN_FAILED_MSG + ", Not a valid operation: unparsed",
            "ACL DENY ALL ALL unparsed, " + AclFileParser.PARSE_TOKEN_FAILED_MSG + ", Not a valid object type: unparsed"
    })
    void ACLFileSyntaxTokens(final String rule, final String expectedErrorMessage, final String expectedCause)
    {
        final IllegalConfigurationException thrown =
                assertThrows(IllegalConfigurationException.class, () -> writeACLConfig(rule), "fail");
        assertEquals(String.format(expectedErrorMessage, 1), thrown.getMessage());
        assertTrue(thrown.getCause() instanceof IllegalArgumentException);
        assertEquals(expectedCause, thrown.getCause().getMessage());
    }

    @Test
    void ACLFileSyntaxNotEnoughACL()
    {
        final IllegalConfigurationException thrown = assertThrows(IllegalConfigurationException.class,
                () -> writeACLConfig("ACL ALLOW"),
                "fail");
        assertEquals(String.format(AclFileParser.NOT_ENOUGH_ACL_MSG, 1), thrown.getMessage());
    }

    @ParameterizedTest
    @CsvSource(
    {
            "CONFIG, " + AclFileParser.NOT_ENOUGH_TOKENS_MSG,
            "CONFIG defaultdeny, " + AclFileParser.NOT_ENOUGH_CONFIG_MSG
    })
    void ACLFileSyntaxNotEnoughConfig(final String rule, final String expectedErrorMessage)
    {
        final IllegalConfigurationException thrown =
                assertThrows(IllegalConfigurationException.class, () -> writeACLConfig(rule), "fail");
        assertEquals(String.format(expectedErrorMessage, 1), thrown.getMessage());
    }

    @Test
    void ACLFileSyntaxOrderedConfig()
    {
        final IllegalConfigurationException thrown = assertThrows(IllegalConfigurationException.class,
                () -> writeACLConfig("3 CONFIG defaultdeny=true"),
                "fail");
        assertEquals(String.format(AclFileParser.NUMBER_NOT_ALLOWED_MSG, "CONFIG", 1), thrown.getMessage());
    }

    @ParameterizedTest
    @CsvSource(
    {
            "CONFIG default=false defaultdeny, " + AclFileParser.PROPERTY_KEY_ONLY_MSG,
            "CONFIG default=false defaultdeny true, " + AclFileParser.PROPERTY_NO_EQUALS_MSG,
            "CONFIG default=false defaultdeny=, " + AclFileParser.PROPERTY_NO_VALUE_MSG
    })
    void ACLFileSyntaxInvalidConfig(final String rule, final String expectedErrorMessage)
    {
        final IllegalConfigurationException thrown =
                assertThrows(IllegalConfigurationException.class, () -> writeACLConfig(rule), "fail");
        assertEquals(String.format(expectedErrorMessage, 1), thrown.getMessage());
    }

    @Test
    void ACLFileSyntaxNotEnough()
    {
        final IllegalConfigurationException thrown = assertThrows(IllegalConfigurationException.class,
                () -> writeACLConfig("INVALID"),
                "fail");
        assertEquals(String.format(AclFileParser.NOT_ENOUGH_TOKENS_MSG, 1), thrown.getMessage());
    }

    @Test
    void ACLFileSyntaxPropertyKeyOnly()
    {
        final IllegalConfigurationException thrown = assertThrows(IllegalConfigurationException.class,
                () -> writeACLConfig("ACL ALLOW adk CREATE QUEUE name"),
                "fail");
        assertEquals(String.format(AclFileParser.PROPERTY_KEY_ONLY_MSG, 1), thrown.getMessage());
    }

    @Test
    void ACLFileSyntaxPropertyNoEquals()
    {
        final IllegalConfigurationException thrown = assertThrows(IllegalConfigurationException.class,
                () -> writeACLConfig("ACL ALLOW adk CREATE QUEUE name test"),
                "fail");
        assertEquals(String.format(AclFileParser.PROPERTY_NO_EQUALS_MSG, 1), thrown.getMessage());
    }

    @Test
    void ACLFileSyntaxPropertyNoValue()
    {
        final IllegalConfigurationException thrown = assertThrows(IllegalConfigurationException.class,
                () -> writeACLConfig("ACL ALLOW adk CREATE QUEUE name ="),
                "fail");
        assertEquals(String.format(AclFileParser.PROPERTY_NO_VALUE_MSG, 1), thrown.getMessage());
    }

    @ParameterizedTest
    @CsvSource(
    {
            "CONFIG defaultdefer=true, DEFER",
            "CONFIG defaultdeny=true, DENIED",
            "CONFIG defaultallow=true, ALLOWED",
            "CONFIG defaultdefer=false defaultallow=true defaultdeny=false df=false, ALLOWED"
    })
    void validConfig(final String rule, final Result expectedResult)
    {
        final RuleSet ruleSet = writeACLConfig(rule);
        assertEquals(0, ruleSet.size(), "Unexpected number of rules");
        assertEquals(expectedResult, ruleSet.getDefault(), "Unexpected default outcome");
    }

    /**
     * Tests interpretation of an acl rule with no object properties.
     */
    @Test
    void validRule()
    {
        final RuleSet rules = writeACLConfig("ACL DENY-LOG user1 ACCESS VIRTUALHOST");
        assertEquals(1, rules.size());

        final Rule rule = rules.get(0);
        assertEquals("user1", rule.getIdentity(), "Rule has unexpected identity");
        assertEquals(LegacyOperation.ACCESS, rule.getOperation(), "Rule has unexpected operation");
        assertEquals(ObjectType.VIRTUALHOST, rule.getObjectType(), "Rule has unexpected object type");
        assertEquals(EMPTY, rule.getPredicates(), "Rule has unexpected predicates");
    }

    /**
     * Tests interpretation of an acl rule with object properties quoted in single quotes.
     */
    @Test
    void validRuleWithSingleQuotedProperty()
    {
        final RuleSet rules = writeACLConfig("ACL ALLOW all CREATE EXCHANGE name = 'value'");
        assertEquals(1, rules.size());

        final Rule rule = rules.get(0);
        assertEquals("all", rule.getIdentity(), "Rule has unexpected identity");
        assertEquals(LegacyOperation.CREATE, rule.getOperation(), "Rule has unexpected operation");
        assertEquals(ObjectType.EXCHANGE, rule.getObjectType(), "Rule has unexpected object type");
        final AclRulePredicates expectedPredicates = new AclRulePredicatesBuilder()
                .put(Property.NAME, "value").build();
        assertEquals(expectedPredicates, rule.getPredicates(), "Rule has unexpected predicates");
    }

    /**
     * Tests interpretation of an acl rule with object properties quoted in double quotes.
     */
    @Test
    void validRuleWithDoubleQuotedProperty()
    {
        final RuleSet rules = writeACLConfig("ACL ALLOW all CREATE EXCHANGE name = \"value\"");

        assertEquals(1, rules.size());
        final Rule rule = rules.get(0);
        assertEquals("all", rule.getIdentity(), "Rule has unexpected identity");
        assertEquals(LegacyOperation.CREATE, rule.getOperation(), "Rule has unexpected operation");
        assertEquals(ObjectType.EXCHANGE, rule.getObjectType(), "Rule has unexpected object type");
        final AclRulePredicates expectedPredicates = new AclRulePredicatesBuilder()
                .put(Property.NAME, "value").build();
        assertEquals(expectedPredicates, rule.getPredicates(), "Rule has unexpected predicates");
    }

    /**
     * Tests interpretation of an acl rule with many object properties.
     */
    @Test
    void validRuleWithManyProperties()
    {
        final RuleSet rules = writeACLConfig("ACL ALLOW admin DELETE QUEUE name=name1 owner = owner1");
        assertEquals(1, rules.size());

        final Rule rule = rules.get(0);
        assertEquals("admin", rule.getIdentity(), "Rule has unexpected identity");
        assertEquals(LegacyOperation.DELETE, rule.getOperation(), "Rule has unexpected operation");
        assertEquals(ObjectType.QUEUE, rule.getObjectType(), "Rule has unexpected object type");
        final AclRulePredicates expectedPredicates = new AclRulePredicatesBuilder()
                .put(Property.NAME, "name1")
                .put(Property.OWNER, "owner1").build();
        assertEquals(expectedPredicates, rule.getPredicates(), "Rule has unexpected operation");
    }

    /**
     * Tests interpretation of an acl rule with object properties containing wildcards.  Values containing
     * hashes must be quoted otherwise they are interpreted as comments.
     */
    @Test
    void validRuleWithWildcardProperties()
    {
        final RuleSet rules = writeACLConfig("ACL ALLOW all CREATE EXCHANGE routingKey = 'news.#'",
                "ACL ALLOW all CREATE EXCHANGE routingKey = 'news.co.#'",
                "ACL ALLOW all CREATE EXCHANGE routingKey = *.co.medellin");
        assertEquals(3, rules.size());

        final Rule rule1 = rules.get(0);
        assertEquals("all", rule1.getIdentity(), "Rule has unexpected identity");
        assertEquals(LegacyOperation.CREATE, rule1.getOperation(), "Rule has unexpected operation");
        assertEquals(ObjectType.EXCHANGE, rule1.getObjectType(), "Rule has object type");
        final AclRulePredicates expectedPredicates1 = new AclRulePredicatesBuilder()
                .put(Property.ROUTING_KEY, "news.#").build();
        assertEquals(expectedPredicates1, rule1.getPredicates(), "Rule has unexpected predicates");

        final Rule rule2 = rules.get(1);
        final AclRulePredicates expectedPredicates2 = new AclRulePredicatesBuilder()
                .put(Property.ROUTING_KEY, "news.co.#").build();
        assertEquals(expectedPredicates2, rule2.getPredicates(), "Rule has unexpected predicates");

        final Rule rule3 = rules.get(2);
        final AclRulePredicates expectedPredicates3 = new AclRulePredicatesBuilder()
                .put(Property.ROUTING_KEY, "*.co.medellin").build();
        assertEquals(expectedPredicates3, rule3.getPredicates(), "Rule has unexpected predicates");
    }

    @Test
    void orderedValidRule()
    {
        final RuleSet rules = writeACLConfig("5 ACL DENY all CREATE EXCHANGE",
                "3 ACL ALLOW all CREATE EXCHANGE routingKey = 'news.co.#'",
                "1 ACL ALLOW all CREATE EXCHANGE routingKey = *.co.medellin");
        assertEquals(3, rules.size());

        final Rule rule1 = rules.get(0);
        final AclRulePredicates expectedPredicates1 = new AclRulePredicatesBuilder()
                .put(Property.ROUTING_KEY, "*.co.medellin").build();
        assertEquals(expectedPredicates1, rule1.getPredicates(), "Rule has unexpected predicates");

        final Rule rule3 = rules.get(1);
        final AclRulePredicates expectedPredicates3 = new AclRulePredicatesBuilder()
                .put(Property.ROUTING_KEY, "news.co.#").build();
        assertEquals(expectedPredicates3, rule3.getPredicates(), "Rule has unexpected predicates");

        final Rule rule5 = rules.get(2);
        assertEquals("all", rule5.getIdentity(), "Rule has unexpected identity");
        assertEquals(LegacyOperation.CREATE, rule5.getOperation(), "Rule has unexpected operation");
        assertEquals(ObjectType.EXCHANGE, rule5.getObjectType(), "Rule has unexpected object type");
        final AclRulePredicates expectedPredicates5 = new AclRulePredicatesBuilder().build();
        assertEquals(expectedPredicates5, rule5.getPredicates(), "Rule has unexpected predicates");
    }

    @Test
    void orderedInValidRule()
    {
        final IllegalConfigurationException thrown = assertThrows(IllegalConfigurationException.class,
                () -> writeACLConfig("5 ACL DENY all CREATE EXCHANGE",
                        "3 ACL ALLOW all CREATE EXCHANGE routingKey = 'news.co.#'",
                        "5 ACL ALLOW all CREATE EXCHANGE routingKey = *.co.medellin"),
                "fail");
        assertEquals(String.format(AclFileParser.BAD_ACL_RULE_NUMBER_MSG, 3), thrown.getMessage());
    }

    @Test
    void shortValidRule()
    {
        final RuleSet rules = writeACLConfig("ACL DENY user UPDATE");
        assertEquals(1, rules.size());
        validateRule(rules, "user", LegacyOperation.UPDATE, ObjectType.ALL, EMPTY);
    }

    /**
     * Tests that rules are case insignificant.
     */
    @Test
    void mixedCaseRuleInterpretation()
    {
        final RuleSet rules = writeACLConfig("AcL deny-LOG User1 BiND Exchange Name=AmQ.dIrect");
        assertEquals(1, rules.size());

        final Rule rule = rules.get(0);
        assertEquals("User1", rule.getIdentity(), "Rule has unexpected identity");
        assertEquals(LegacyOperation.BIND, rule.getOperation(), "Rule has unexpected operation");
        assertEquals(ObjectType.EXCHANGE, rule.getObjectType(), "Rule has unexpected object type");
        final AclRulePredicates expectedPredicates = new AclRulePredicatesBuilder()
                .put(Property.NAME, "AmQ.dIrect").build();
        assertEquals(expectedPredicates, rule.getPredicates(), "Rule has unexpected predicates");
    }

    /**
     * Tests whitespace is supported. Note that currently the Java implementation permits comments to
     * be introduced anywhere in the ACL, whereas the C++ supports only whitespace at the beginning of
     * of line.
     */
    @Test
    void commentsSupported()
    {
        final RuleSet rules = writeACLConfig("#Comment",
                "ACL DENY-LOG user1 ACCESS VIRTUALHOST # another comment",
                "  # final comment with leading whitespace");
        assertEquals(1, rules.size());

        final Rule rule = rules.get(0);
        assertEquals("user1", rule.getIdentity(), "Rule has unexpected identity");
        assertEquals(LegacyOperation.ACCESS, rule.getOperation(), "Rule has unexpected operation");
        assertEquals(ObjectType.VIRTUALHOST, rule.getObjectType(), "Rule has unexpected object type");
        assertEquals(EMPTY, rule.getPredicates(), "Rule has unexpected predicates");
    }

    /**
     * Tests interpretation of an acl rule using mixtures of tabs/spaces as token separators.
     */
    @ParameterizedTest
    @ValueSource(strings =
    {
            "ACL\tDENY-LOG\t\t user1\t \tACCESS VIRTUALHOST",
            "ACL\u000B DENY-LOG\t\t user1\t \tACCESS VIRTUALHOST\u001E"
    })
    void whitespaces(final String arg)
    {
        final RuleSet rules = writeACLConfig(arg);
        assertEquals(1, rules.size());

        final Rule rule = rules.get(0);
        assertEquals("user1", rule.getIdentity(), "Rule has unexpected identity");
        assertEquals(LegacyOperation.ACCESS, rule.getOperation(), "Rule has unexpected operation");
        assertEquals(ObjectType.VIRTUALHOST, rule.getObjectType(), "Rule has unexpected object type");
        assertEquals(EMPTY, rule.getPredicates(), "Rule has unexpected predicates");
    }

    /**
     * Tests interpretation of an acl utilising line continuation.
     */
    @Test
    void lineContinuation()
    {
        final RuleSet rules = writeACLConfig("ACL DENY-LOG user1 \\", "ACCESS VIRTUALHOST");
        assertEquals(1, rules.size());

        final Rule rule = rules.get(0);
        assertEquals("user1", rule.getIdentity(), "Rule has unexpected identity");
        assertEquals(LegacyOperation.ACCESS, rule.getOperation(), "Rule has unexpected operation");
        assertEquals(ObjectType.VIRTUALHOST, rule.getObjectType(), "Rule has unexpected object type");
        assertEquals(EMPTY, rule.getPredicates(), "Rule has unexpected predicates");
    }

    @Test
    void userRuleParsing()
    {
        final AclRulePredicates predicates = new AclRulePredicatesBuilder()
                .put(Property.NAME, "otherUser").build();

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
    void groupRuleParsing()
    {
        final AclRulePredicates predicates = new AclRulePredicatesBuilder()
                .put(Property.NAME, "groupName").build();

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
     * Explicitly test for exception indicating that this functionality has been moved to Group Providers
     */
    @Test
    void groupDefinitionThrowsException()
    {
        final IllegalConfigurationException thrown = assertThrows(IllegalConfigurationException.class,
                () -> writeACLConfig("GROUP group1 bob alice"),
                "Expected exception not thrown");
        assertTrue(thrown.getMessage().contains("GROUP keyword not supported"));
    }

    @Test
    void unknownDefinitionThrowsException()
    {
        final IllegalConfigurationException thrown = assertThrows(IllegalConfigurationException.class,
                () -> writeACLConfig("Unknown group1 bob alice"),
                "Expected exception not thrown");
        assertEquals(String.format(AclFileParser.UNRECOGNISED_INITIAL_MSG, "Unknown", 1), thrown.getMessage());
    }

    @Test
    void managementRuleParsing()
    {
        validateRule(writeACLConfig("ACL ALLOW user1 ALL MANAGEMENT"),
                "user1", LegacyOperation.ALL, ObjectType.MANAGEMENT, EMPTY);

        validateRule(writeACLConfig("ACL ALLOW user1 ACCESS MANAGEMENT"),
                "user1", LegacyOperation.ACCESS, ObjectType.MANAGEMENT, EMPTY);
    }

    @ParameterizedTest
    @CsvSource(
    {
            "ACL ALLOW user1 CONFIGURE BROKER, CONFIGURE",
            "ACL ALLOW user1 ALL BROKER, ALL"
    })
    void brokerRuleParsing(final String rule, final LegacyOperation expectedOperation)
    {
        validateRule(writeACLConfig(rule), "user1", expectedOperation, ObjectType.BROKER, EMPTY);
    }

    @Test
    void readerIOException() throws IOException
    {
        final Reader reader = mock(Reader.class);
        doReturn(true).when(reader).ready();
        doReturn(1L).when(reader).skip(Mockito.anyLong());
        doThrow(IOException.class).when(reader).read();
        doThrow(IOException.class).when(reader).read(Mockito.any(CharBuffer.class));
        doThrow(IOException.class).when(reader).read(Mockito.any(char[].class));
        doThrow(IOException.class).when(reader)
                .read(Mockito.any(char[].class), Mockito.anyInt(), Mockito.anyInt());

        final AclFileParser aclFileParser =  new AclFileParser();
        final IllegalConfigurationException thrown = assertThrows(IllegalConfigurationException.class,
                () -> aclFileParser.readAndParse(reader),
                "Expected exception not thrown");
        assertEquals(AclFileParser.CANNOT_LOAD_MSG, thrown.getMessage());
    }

    @Test
    void readerRuntimeException() throws IOException
    {
        final Reader reader = mock(Reader.class);
        doReturn(true).when(reader).ready();
        doReturn(1L).when(reader).skip(Mockito.anyLong());
        doThrow(RuntimeException.class).when(reader).read();
        doThrow(RuntimeException.class).when(reader).read(Mockito.any(CharBuffer.class));
        doThrow(RuntimeException.class).when(reader).read(Mockito.any(char[].class));
        doThrow(RuntimeException.class).when(reader)
                .read(Mockito.any(char[].class), Mockito.anyInt(), Mockito.anyInt());

        final AclFileParser aclFileParser =  new AclFileParser();
        final IllegalConfigurationException thrown = assertThrows(IllegalConfigurationException.class,
                () -> aclFileParser.readAndParse(reader),
                "Expected exception not thrown");
        assertEquals(String.format(AclFileParser.PARSE_TOKEN_FAILED_MSG, 0), thrown.getMessage());
    }

    private void validateRule(final RuleSet rules,
                              final String username,
                              final LegacyOperation operation,
                              final ObjectType objectType,
                              final AclRulePredicates predicates)
    {
        assertEquals(1, rules.size());
        final Rule rule = rules.get(0);
        assertEquals(username, rule.getIdentity(), "Rule has unexpected identity");
        assertEquals(operation, rule.getOperation(), "Rule has unexpected operation");
        assertEquals(objectType, rule.getObjectType(), "Rule has unexpected object type");
        assertEquals(predicates, rule.getPredicates(), "Rule has unexpected predicates");
    }

    @Test
    void connectionLimitParsing()
    {
        validateRule(writeACLConfig("ACL ALLOW all ACCESS VIRTUALHOST connection_limit=10 connection_frequency_limit=12"),
                Rule.ALL,
                LegacyOperation.ACCESS,
                ObjectType.VIRTUALHOST,
                EMPTY);
    }
}