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
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.List;

import org.junit.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.security.access.config.ObjectProperties.Property;
import org.apache.qpid.test.utils.UnitTestBase;

public class AclFileParserTest extends UnitTestBase
{
    private RuleSet writeACLConfig(String...aclData) throws Exception
    {
        File acl = File.createTempFile(getClass().getName() + getTestName(), "acl");
        acl.deleteOnExit();

        // Write ACL file
        try (PrintWriter aclWriter = new PrintWriter(new FileWriter(acl)))
        {
            for (String line : aclData)
            {
                aclWriter.println(line);
            }
        }

        // Load ruleset
        return AclFileParser.parse(new FileReader(acl), mock(EventLoggerProvider.class));
    }

    @Test
    public void testEmptyRuleSetDefaults() throws Exception
    {
        RuleSet ruleSet = writeACLConfig();
        assertEquals((long) 0, (long) ruleSet.getRuleCount());
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
            final boolean condition = ce.getCause() instanceof IllegalArgumentException;
            assertTrue(condition);
            assertEquals("Not a valid permission: unparsed", ce.getCause().getMessage());
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
        assertEquals("Unexpected number of rules", (long) 0, (long) ruleSet.getRuleCount());
        assertEquals("Unexpected number of rules", Result.DEFER, ruleSet.getDefault());
    }

    /**
     * Tests interpretation of an acl rule with no object properties.
     *
     */
    @Test
    public void testValidRule() throws Exception
    {
        final RuleSet rs = writeACLConfig("ACL DENY-LOG user1 ACCESS VIRTUALHOST");
        assertEquals((long) 1, (long) rs.getRuleCount());

        final List<Rule> rules = rs.getAllRules();
        assertEquals((long) 1, (long) rules.size());
        final Rule rule = rules.get(0);
        assertEquals("Rule has unexpected identity", "user1", rule.getIdentity());
        assertEquals("Rule has unexpected operation", LegacyOperation.ACCESS, rule.getAction().getOperation());
        assertEquals("Rule has unexpected operation", ObjectType.VIRTUALHOST, rule.getAction().getObjectType());
        assertEquals("Rule has unexpected object properties",
                            ObjectProperties.EMPTY,
                            rule.getAction().getProperties());
    }

    /**
     * Tests interpretation of an acl rule with object properties quoted in single quotes.
     */
    @Test
    public void testValidRuleWithSingleQuotedProperty() throws Exception
    {
        final RuleSet rs = writeACLConfig("ACL ALLOW all CREATE EXCHANGE name = \'value\'");
        assertEquals((long) 1, (long) rs.getRuleCount());

        final List<Rule> rules = rs.getAllRules();
        assertEquals((long) 1, (long) rules.size());
        final Rule rule = rules.get(0);
        assertEquals("Rule has unexpected identity", "all", rule.getIdentity());
        assertEquals("Rule has unexpected operation", LegacyOperation.CREATE, rule.getAction().getOperation());
        assertEquals("Rule has unexpected operation", ObjectType.EXCHANGE, rule.getAction().getObjectType());
        final ObjectProperties expectedProperties = new ObjectProperties();
        expectedProperties.setName("value");
        assertEquals("Rule has unexpected object properties",
                            expectedProperties,
                            rule.getAction().getProperties());
    }

    /**
     * Tests interpretation of an acl rule with object properties quoted in double quotes.
     */
    @Test
    public void testValidRuleWithDoubleQuotedProperty() throws Exception
    {
        final RuleSet rs = writeACLConfig("ACL ALLOW all CREATE EXCHANGE name = \"value\"");
        assertEquals((long) 1, (long) rs.getRuleCount());

        final List<Rule> rules = rs.getAllRules();
        assertEquals((long) 1, (long) rules.size());
        final Rule rule = rules.get(0);
        assertEquals("Rule has unexpected identity", "all", rule.getIdentity());
        assertEquals("Rule has unexpected operation", LegacyOperation.CREATE, rule.getAction().getOperation());
        assertEquals("Rule has unexpected operation", ObjectType.EXCHANGE, rule.getAction().getObjectType());
        final ObjectProperties expectedProperties = new ObjectProperties();
        expectedProperties.setName("value");
        assertEquals("Rule has unexpected object properties",
                            expectedProperties,
                            rule.getAction().getProperties());
    }

    /**
     * Tests interpretation of an acl rule with many object properties.
     */
    @Test
    public void testValidRuleWithManyProperties() throws Exception
    {
        final RuleSet rs = writeACLConfig("ACL ALLOW admin DELETE QUEUE name=name1 owner = owner1");
        assertEquals((long) 1, (long) rs.getRuleCount());

        final List<Rule> rules = rs.getAllRules();
        assertEquals((long) 1, (long) rules.size());
        final Rule rule = rules.get(0);
        assertEquals("Rule has unexpected identity", "admin", rule.getIdentity());
        assertEquals("Rule has unexpected operation", LegacyOperation.DELETE, rule.getAction().getOperation());
        assertEquals("Rule has unexpected operation", ObjectType.QUEUE, rule.getAction().getObjectType());
        final ObjectProperties expectedProperties = new ObjectProperties();
        expectedProperties.setName("name1");
        expectedProperties.put(Property.OWNER, "owner1");
        assertEquals("Rule has unexpected operation", expectedProperties, rule.getAction().getProperties());
    }

    /**
     * Tests interpretation of an acl rule with object properties containing wildcards.  Values containing
     * hashes must be quoted otherwise they are interpreted as comments.
     */
    @Test
    public void testValidRuleWithWildcardProperties() throws Exception
    {
        final RuleSet rs = writeACLConfig("ACL ALLOW all CREATE EXCHANGE routingKey = \'news.#\'",
                                                         "ACL ALLOW all CREATE EXCHANGE routingKey = \'news.co.#\'",
                                                         "ACL ALLOW all CREATE EXCHANGE routingKey = *.co.medellin");
        assertEquals((long) 3, (long) rs.getRuleCount());

        final List<Rule> rules = rs.getAllRules();
        assertEquals((long) 3, (long) rules.size());
        final Rule rule1 = rules.get(0);
        assertEquals("Rule has unexpected identity", "all", rule1.getIdentity());
        assertEquals("Rule has unexpected operation", LegacyOperation.CREATE, rule1.getAction().getOperation());
        assertEquals("Rule has unexpected operation", ObjectType.EXCHANGE, rule1.getAction().getObjectType());
        final ObjectProperties expectedProperties1 = new ObjectProperties();
        expectedProperties1.put(Property.ROUTING_KEY,"news.#");
        assertEquals("Rule has unexpected object properties",
                            expectedProperties1,
                            rule1.getAction().getProperties());

        final Rule rule2 = rules.get(1);
        final ObjectProperties expectedProperties2 = new ObjectProperties();
        expectedProperties2.put(Property.ROUTING_KEY,"news.co.#");
        assertEquals("Rule has unexpected object properties",
                            expectedProperties2,
                            rule2.getAction().getProperties());

        final Rule rule3 = rules.get(2);
        final ObjectProperties expectedProperties3 = new ObjectProperties();
        expectedProperties3.put(Property.ROUTING_KEY,"*.co.medellin");
        assertEquals("Rule has unexpected object properties",
                            expectedProperties3,
                            rule3.getAction().getProperties());
    }

    /**
     * Tests that rules are case insignificant.
     */
    @Test
    public void testMixedCaseRuleInterpretation() throws Exception
    {
        final RuleSet rs  = writeACLConfig("AcL deny-LOG User1 BiND Exchange Name=AmQ.dIrect");
        assertEquals((long) 1, (long) rs.getRuleCount());

        final List<Rule> rules = rs.getAllRules();
        assertEquals((long) 1, (long) rules.size());
        final Rule rule = rules.get(0);
        assertEquals("Rule has unexpected identity", "User1", rule.getIdentity());
        assertEquals("Rule has unexpected operation", LegacyOperation.BIND, rule.getAction().getOperation());
        assertEquals("Rule has unexpected operation", ObjectType.EXCHANGE, rule.getAction().getObjectType());
        final ObjectProperties expectedProperties = new ObjectProperties("AmQ.dIrect");
        assertEquals("Rule has unexpected object properties",
                            expectedProperties,
                            rule.getAction().getProperties());
    }

    /**
     * Tests whitespace is supported. Note that currently the Java implementation permits comments to
     * be introduced anywhere in the ACL, whereas the C++ supports only whitespace at the beginning of
     * of line.
     */
    @Test
    public void testCommentsSupported() throws Exception
    {
        final RuleSet rs = writeACLConfig("#Comment",
                                                         "ACL DENY-LOG user1 ACCESS VIRTUALHOST # another comment",
                                                         "  # final comment with leading whitespace");
        assertEquals((long) 1, (long) rs.getRuleCount());

        final List<Rule> rules = rs.getAllRules();
        assertEquals((long) 1, (long) rules.size());
        final Rule rule = rules.get(0);
        assertEquals("Rule has unexpected identity", "user1", rule.getIdentity());
        assertEquals("Rule has unexpected operation", LegacyOperation.ACCESS, rule.getAction().getOperation());
        assertEquals("Rule has unexpected operation", ObjectType.VIRTUALHOST, rule.getAction().getObjectType());
        assertEquals("Rule has unexpected object properties",
                            ObjectProperties.EMPTY,
                            rule.getAction().getProperties());
    }

    /**
     * Tests interpretation of an acl rule using mixtures of tabs/spaces as token separators.
     *
     */
    @Test
    public void testWhitespace() throws Exception
    {
        final RuleSet rs = writeACLConfig("ACL\tDENY-LOG\t\t user1\t \tACCESS VIRTUALHOST");
        assertEquals((long) 1, (long) rs.getRuleCount());

        final List<Rule> rules = rs.getAllRules();
        assertEquals((long) 1, (long) rules.size());
        final Rule rule = rules.get(0);
        assertEquals("Rule has unexpected identity", "user1", rule.getIdentity());
        assertEquals("Rule has unexpected operation", LegacyOperation.ACCESS, rule.getAction().getOperation());
        assertEquals("Rule has unexpected operation", ObjectType.VIRTUALHOST, rule.getAction().getObjectType());
        assertEquals("Rule has unexpected object properties",
                            ObjectProperties.EMPTY,
                            rule.getAction().getProperties());
    }

    /**
     * Tests interpretation of an acl utilising line continuation.
     */
    @Test
    public void testLineContinuation() throws Exception
    {
        final RuleSet rs = writeACLConfig("ACL DENY-LOG user1 \\",
                                                         "ACCESS VIRTUALHOST");
        assertEquals((long) 1, (long) rs.getRuleCount());

        final List<Rule> rules = rs.getAllRules();
        assertEquals((long) 1, (long) rules.size());
        final Rule rule = rules.get(0);
        assertEquals("Rule has unexpected identity", "user1", rule.getIdentity());
        assertEquals("Rule has unexpected operation", LegacyOperation.ACCESS, rule.getAction().getOperation());
        assertEquals("Rule has unexpected operation", ObjectType.VIRTUALHOST, rule.getAction().getObjectType());
        assertEquals("Rule has unexpected object properties",
                            ObjectProperties.EMPTY,
                            rule.getAction().getProperties());
    }

    @Test
    public void testUserRuleParsing() throws Exception
    {
        validateRule(writeACLConfig("ACL ALLOW user1 CREATE USER"),
                     "user1", LegacyOperation.CREATE, ObjectType.USER, ObjectProperties.EMPTY);
        validateRule(writeACLConfig("ACL ALLOW user1 CREATE USER name=\"otherUser\""),
                     "user1", LegacyOperation.CREATE, ObjectType.USER, new ObjectProperties("otherUser"));

        validateRule(writeACLConfig("ACL ALLOW user1 DELETE USER"),
                     "user1", LegacyOperation.DELETE, ObjectType.USER, ObjectProperties.EMPTY);
        validateRule(writeACLConfig("ACL ALLOW user1 DELETE USER name=\"otherUser\""),
                     "user1", LegacyOperation.DELETE, ObjectType.USER, new ObjectProperties("otherUser"));

        validateRule(writeACLConfig("ACL ALLOW user1 UPDATE USER"),
                     "user1", LegacyOperation.UPDATE, ObjectType.USER, ObjectProperties.EMPTY);
        validateRule(writeACLConfig("ACL ALLOW user1 UPDATE USER name=\"otherUser\""),
                     "user1", LegacyOperation.UPDATE, ObjectType.USER, new ObjectProperties("otherUser"));

        validateRule(writeACLConfig("ACL ALLOW user1 ALL USER"),
                     "user1", LegacyOperation.ALL, ObjectType.USER, ObjectProperties.EMPTY);
        validateRule(writeACLConfig("ACL ALLOW user1 ALL USER name=\"otherUser\""),
                     "user1", LegacyOperation.ALL, ObjectType.USER, new ObjectProperties("otherUser"));
    }

    @Test
    public void testGroupRuleParsing() throws Exception
    {
        validateRule(writeACLConfig("ACL ALLOW user1 CREATE GROUP"),
                     "user1", LegacyOperation.CREATE, ObjectType.GROUP, ObjectProperties.EMPTY);
        validateRule(writeACLConfig("ACL ALLOW user1 CREATE GROUP name=\"groupName\""),
                     "user1", LegacyOperation.CREATE, ObjectType.GROUP, new ObjectProperties("groupName"));

        validateRule(writeACLConfig("ACL ALLOW user1 DELETE GROUP"),
                     "user1", LegacyOperation.DELETE, ObjectType.GROUP, ObjectProperties.EMPTY);
        validateRule(writeACLConfig("ACL ALLOW user1 DELETE GROUP name=\"groupName\""),
                     "user1", LegacyOperation.DELETE, ObjectType.GROUP, new ObjectProperties("groupName"));

        validateRule(writeACLConfig("ACL ALLOW user1 UPDATE GROUP"),
                     "user1", LegacyOperation.UPDATE, ObjectType.GROUP, ObjectProperties.EMPTY);
        validateRule(writeACLConfig("ACL ALLOW user1 UPDATE GROUP name=\"groupName\""),
                     "user1", LegacyOperation.UPDATE, ObjectType.GROUP, new ObjectProperties("groupName"));

        validateRule(writeACLConfig("ACL ALLOW user1 ALL GROUP"),
                     "user1", LegacyOperation.ALL, ObjectType.GROUP, ObjectProperties.EMPTY);
        validateRule(writeACLConfig("ACL ALLOW user1 ALL GROUP name=\"groupName\""),
                     "user1", LegacyOperation.ALL, ObjectType.GROUP, new ObjectProperties("groupName"));
    }

    /** explicitly test for exception indicating that this functionality has been moved to Group Providers */
    @Test
    public void testGroupDefinitionThrowsException() throws Exception
    {
        try
        {
            writeACLConfig("GROUP group1 bob alice");
            fail("Expected exception not thrown");
        }
        catch(IllegalConfigurationException e)
        {
            assertTrue(e.getMessage().contains("GROUP keyword not supported"));
        }
    }

    @Test
    public void testManagementRuleParsing() throws Exception
    {
        validateRule(writeACLConfig("ACL ALLOW user1 ALL MANAGEMENT"),
                     "user1", LegacyOperation.ALL, ObjectType.MANAGEMENT, ObjectProperties.EMPTY);

        validateRule(writeACLConfig("ACL ALLOW user1 ACCESS MANAGEMENT"),
                     "user1", LegacyOperation.ACCESS, ObjectType.MANAGEMENT, ObjectProperties.EMPTY);
    }

    @Test
    public void testBrokerRuleParsing() throws Exception
    {
        validateRule(writeACLConfig("ACL ALLOW user1 CONFIGURE BROKER"), "user1", LegacyOperation.CONFIGURE, ObjectType.BROKER,
                     ObjectProperties.EMPTY);
        validateRule(writeACLConfig("ACL ALLOW user1 ALL BROKER"), "user1", LegacyOperation.ALL, ObjectType.BROKER, ObjectProperties.EMPTY);
    }

    private void validateRule(final RuleSet rs, String username, LegacyOperation operation, ObjectType objectType, ObjectProperties objectProperties)
    {
        assertEquals((long) 1, (long) rs.getRuleCount());

        final List<Rule> rules = rs.getAllRules();
        assertEquals((long) 1, (long) rules.size());
        final Rule rule = rules.get(0);
        assertEquals("Rule has unexpected identity", username, rule.getIdentity());
        assertEquals("Rule has unexpected operation", operation, rule.getAction().getOperation());
        assertEquals("Rule has unexpected operation", objectType, rule.getAction().getObjectType());
        assertEquals("Rule has unexpected object properties", objectProperties, rule.getAction().getProperties());
    }
}
