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
package org.apache.qpid.server.security.access.config;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.security.access.plugins.RuleOutcome;

public final class AclFileParser
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AclFileParser.class);
    public static final String DEFAULT_ALLOW = "defaultallow";
    public static final String DEFAULT_DEFER = "defaultdefer";
    public static final String DEFAULT_DENY = "defaultdeny";

    private static final Character COMMENT = '#';
    private static final Character CONTINUATION = '\\';

    public static final String ACL = "acl";
    private static final String CONFIG = "config";

    private static final String UNRECOGNISED_INITIAL_MSG = "Unrecognised initial token '%s' at line %d";
    static final String NOT_ENOUGH_TOKENS_MSG = "Not enough tokens at line %d";
    private static final String NUMBER_NOT_ALLOWED_MSG = "Number not allowed before '%s' at line %d";
    private static final String CANNOT_LOAD_MSG = "I/O Error while reading configuration";
    static final String PREMATURE_CONTINUATION_MSG = "Premature continuation character at line %d";
    private static final String PREMATURE_EOF_MSG = "Premature end of file reached at line %d";
    static final String PARSE_TOKEN_FAILED_MSG = "Failed to parse token at line %d";
    static final String NOT_ENOUGH_ACL_MSG = "Not enough data for an acl at line %d";
    private static final String NOT_ENOUGH_CONFIG_MSG = "Not enough data for config at line %d";
    private static final String BAD_ACL_RULE_NUMBER_MSG = "Invalid rule number at line %d";
    static final String PROPERTY_KEY_ONLY_MSG = "Incomplete property (key only) at line %d";
    static final String PROPERTY_NO_EQUALS_MSG = "Incomplete property (no equals) at line %d";
    static final String PROPERTY_NO_VALUE_MSG = "Incomplete property (no value) at line %d";


    private AclFileParser()
    {
    }

    private static Reader getReaderFromURLString(String urlString)
    {
        try
        {
            URL url;

            try
            {
                url = new URL(urlString);
            }
            catch (MalformedURLException e)
            {
                File file = new File(urlString);
                try
                {
                    url = file.toURI().toURL();
                }
                catch (MalformedURLException notAFile)
                {
                    throw new IllegalConfigurationException("Cannot convert " + urlString + " to a readable resource", notAFile);
                }

            }
            return new InputStreamReader(url.openStream());
        }
        catch (IOException e)
        {
            throw new IllegalConfigurationException("Cannot convert " + urlString + " to a readable resource", e);
        }
    }

    public static RuleSet parse(String name, EventLoggerProvider eventLoggerProvider)
    {
        return parse(getReaderFromURLString(name), eventLoggerProvider);
    }

    public static RuleSet parse(final Reader configReader, EventLoggerProvider eventLogger)
    {
        RuleSetCreator ruleSetCreator = new RuleSetCreator();

        int line = 0;
        try(Reader fileReader = configReader)
        {
            LOGGER.debug("About to load ACL file");
            StreamTokenizer tokenizer = new StreamTokenizer(new BufferedReader(fileReader));
            tokenizer.resetSyntax(); // setup the tokenizer

            tokenizer.commentChar(COMMENT); // single line comments
            tokenizer.eolIsSignificant(true); // return EOL as a token
            tokenizer.ordinaryChar('='); // equals is a token
            tokenizer.ordinaryChar(CONTINUATION); // continuation character (when followed by EOL)
            tokenizer.quoteChar('"'); // double quote
            tokenizer.quoteChar('\''); // single quote
            tokenizer.whitespaceChars('\u0000', '\u0020'); // whitespace (to be ignored) TODO properly
            tokenizer.wordChars('a', 'z'); // unquoted token characters [a-z]
            tokenizer.wordChars('A', 'Z'); // [A-Z]
            tokenizer.wordChars('0', '9'); // [0-9]
            tokenizer.wordChars('_', '_'); // underscore
            tokenizer.wordChars('-', '-'); // dash
            tokenizer.wordChars('.', '.'); // dot
            tokenizer.wordChars('*', '*'); // star
            tokenizer.wordChars('@', '@'); // at
            tokenizer.wordChars(':', ':'); // colon

            // parse the acl file lines
            Stack<String> stack = new Stack<>();
            int current;
            do {
                current = tokenizer.nextToken();
                line = tokenizer.lineno()-1;
                switch (current)
                {
                    case StreamTokenizer.TT_EOF:
                    case StreamTokenizer.TT_EOL:
                        if (stack.isEmpty())
                        {
                            break; // blank line
                        }

                        // pull out the first token from the bottom of the stack and check arguments exist
                        String first = stack.firstElement();
                        stack.removeElementAt(0);
                        if (stack.isEmpty())
                        {
                            throw new IllegalConfigurationException(String.format(NOT_ENOUGH_TOKENS_MSG, line));
                        }

                        // check for and parse optional initial number for ACL lines
                        Integer number = null;
                        if (first != null && first.matches("\\d+"))
                        {
                            // set the acl number and get the next element
                            number = Integer.valueOf(first);
                            first = stack.firstElement();
                            stack.removeElementAt(0);
                        }

                        if (ACL.equalsIgnoreCase(first))
                        {
                            parseAcl(number, stack, ruleSetCreator, line);
                        }
                        else if (number == null)
                        {
                            if("GROUP".equalsIgnoreCase(first))
                            {
                                throw new IllegalConfigurationException(String.format("GROUP keyword not supported at "
                                                                                      + "line %d. Groups should defined "
                                                                                      + "via a Group Provider, not in "
                                                                                      + "the ACL file.",
                                                                                      line));
                            }
                            else if (CONFIG.equalsIgnoreCase(first))
                            {
                                parseConfig(stack, ruleSetCreator, line);
                            }
                            else
                            {
                                throw new IllegalConfigurationException(String.format(UNRECOGNISED_INITIAL_MSG, first, line));
                            }
                        }
                        else
                        {
                            throw new IllegalConfigurationException(String.format(NUMBER_NOT_ALLOWED_MSG, first, line));
                        }

                        // reset stack, start next line
                        stack.clear();
                        break;
                    case StreamTokenizer.TT_NUMBER:
                        stack.push(Integer.toString(Double.valueOf(tokenizer.nval).intValue()));
                        break;
                    case StreamTokenizer.TT_WORD:
                        stack.push(tokenizer.sval); // token
                        break;
                    default:
                        if (tokenizer.ttype == CONTINUATION)
                        {
                            int next = tokenizer.nextToken();
                            line = tokenizer.lineno()-1;
                            if (next == StreamTokenizer.TT_EOL)
                            {
	                            break; // continue reading next line
                            }

                            // invalid location for continuation character (add one to line because we ate the EOL)
                            throw new IllegalConfigurationException(String.format(PREMATURE_CONTINUATION_MSG, line + 1));
                        }
                        else if (tokenizer.ttype == '\'' || tokenizer.ttype == '"')
                        {
                            stack.push(tokenizer.sval); // quoted token
                        }
                        else
                        {
                            stack.push(Character.toString((char) tokenizer.ttype)); // single character
                        }
                }
            } while (current != StreamTokenizer.TT_EOF);

            if (!stack.isEmpty())
            {
                throw new IllegalConfigurationException(String.format(PREMATURE_EOF_MSG, line));
            }
        }
        catch (IllegalArgumentException iae)
        {
            throw new IllegalConfigurationException(String.format(PARSE_TOKEN_FAILED_MSG, line), iae);
        }
        catch (IOException ioe)
        {
            throw new IllegalConfigurationException(CANNOT_LOAD_MSG, ioe);
        }
        return ruleSetCreator.createRuleSet(eventLogger);
    }

    private static void parseAcl(Integer number, List<String> args, final RuleSetCreator ruleSetCreator, final int line)
    {
        if (args.size() < 3)
        {
            throw new IllegalConfigurationException(String.format(NOT_ENOUGH_ACL_MSG, line));
        }

        String text = args.get(0);
        RuleOutcome outcome;

        try
        {
            outcome = RuleOutcome.valueOf(text.replace('-', '_').toUpperCase());
        }
        catch(IllegalArgumentException e)
        {
            throw new IllegalArgumentException("Not a valid permission: " + text, e);
        }
        String identity = args.get(1);
        LegacyOperation operation = LegacyOperation.valueOf(args.get(2).toUpperCase());

        if (number != null && !ruleSetCreator.isValidNumber(number))
        {
            throw new IllegalConfigurationException(String.format(BAD_ACL_RULE_NUMBER_MSG, line));
        }

        if (args.size() == 3)
        {
            ruleSetCreator.addRule(number, identity, outcome, operation);
        }
        else
        {
            ObjectType object = ObjectType.valueOf(args.get(3).toUpperCase());
            AclRulePredicates predicates = toRulePredicates(args.subList(4, args.size()), line);

            ruleSetCreator.addRule(number, identity, outcome, operation, object, predicates);
        }
    }

    private static void parseConfig(List<String> args, final RuleSetCreator ruleSetCreator, final int line)
    {
        if (args.size() < 3)
        {
            throw new IllegalConfigurationException(String.format(NOT_ENOUGH_CONFIG_MSG, line));
        }

        Map<String, Boolean> properties = toPluginProperties(args, line);



        if (Boolean.TRUE.equals(properties.get(DEFAULT_ALLOW)))
        {
            ruleSetCreator.setDefaultResult(Result.ALLOWED);
        }
        if (Boolean.TRUE.equals(properties.get(DEFAULT_DEFER)))
        {
            ruleSetCreator.setDefaultResult(Result.DEFER);
        }
        if (Boolean.TRUE.equals(properties.get(DEFAULT_DENY)))
        {
            ruleSetCreator.setDefaultResult(Result.DENIED);
        }

    }

    private static AclRulePredicates toRulePredicates(List<String> args, final int line)
    {
        AclRulePredicates predicates = new AclRulePredicates();
        Iterator<String> i = args.iterator();
        while (i.hasNext())
        {
            String key = i.next();
            if (!i.hasNext())
            {
                throw new IllegalConfigurationException(String.format(PROPERTY_KEY_ONLY_MSG, line));
            }
            if (!"=".equals(i.next()))
            {
                throw new IllegalConfigurationException(String.format(PROPERTY_NO_EQUALS_MSG, line));
            }
            if (!i.hasNext())
            {
                throw new IllegalConfigurationException(String.format(PROPERTY_NO_VALUE_MSG, line));
            }
            String value = i.next();

            predicates.parse(key, value);
        }
        return predicates;
    }

    /** Converts a {@link List} of "name", "=", "value" tokens into a {@link Map}. */
    private static Map<String, Boolean> toPluginProperties(List<String> args, final int line)
    {
        Map<String, Boolean> properties = new HashMap<>();
        Iterator<String> i = args.iterator();
        while (i.hasNext())
        {
            String key = i.next().toLowerCase();
            if (!i.hasNext())
            {
                throw new IllegalConfigurationException(String.format(PROPERTY_KEY_ONLY_MSG, line));
            }
            if (!"=".equals(i.next()))
            {
                throw new IllegalConfigurationException(String.format(PROPERTY_NO_EQUALS_MSG, line));
            }
            if (!i.hasNext())
            {
                throw new IllegalConfigurationException(String.format(PROPERTY_NO_VALUE_MSG, line));
            }

            // parse property value and save
            Boolean value = Boolean.valueOf(i.next());
            properties.put(key, value);
        }
        return properties;
    }

}
