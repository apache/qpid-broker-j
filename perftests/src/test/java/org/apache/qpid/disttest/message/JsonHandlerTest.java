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
package org.apache.qpid.disttest.message;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.qpid.disttest.client.property.ListPropertyValue;
import org.apache.qpid.disttest.client.property.PropertyValue;
import org.apache.qpid.disttest.json.JsonHandler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class JsonHandlerTest extends UnitTestBase
{
    private JsonHandler _jsonHandler = null;
    private SendChristmasCards _testCommand = null;

    @BeforeEach
    public void setUp() throws Exception
    {
        _jsonHandler = new JsonHandler();
        _testCommand = new SendChristmasCards(CommandType.START_TEST, Collections.singletonMap(SendChristmasCards.CardType.FUNNY, 5));
        _testCommand._persons = Arrays.asList(new Person("Phil"), new Person("Andrew"));
    }

    @Test
    public void testMarshallUnmarshall() throws Exception
    {
        final String jsonString = _jsonHandler.marshall(_testCommand);

        final SendChristmasCards unmarshalledCommand = _jsonHandler.unmarshall(jsonString, SendChristmasCards.class);

        assertEquals(_testCommand, unmarshalledCommand,
                "Unmarshalled command should be equal to the original object");

    }

    @Test
    public void testSimplePropertyValueMarshallUnmarshall() throws Exception
    {
        String json = "{'_messageProperties': {'test': 1}}";
        final TestCommand unmarshalledCommand = _jsonHandler.unmarshall(json, TestCommand.class);

        Map<String, PropertyValue> properties = unmarshalledCommand.getMessageProperties();
        assertNotNull(properties, "Properties should not be null");
        assertFalse(properties.isEmpty(), "Properties should not be empty");
        assertEquals(1, (long) properties.size(), "Unexpected properties size");
        PropertyValue testProperty = properties.get("test");
        assertNotNull(testProperty, "Unexpected property test");
        final boolean condition = testProperty.getValue() instanceof Number;
        assertTrue(condition, "Unexpected property test");
        assertEquals(1, (long) ((Number) testProperty.getValue()).intValue(),
                "Unexpected property value");

        String newJson =_jsonHandler.marshall(unmarshalledCommand);
        final TestCommand newUnmarshalledCommand = _jsonHandler.unmarshall(newJson, TestCommand.class);
        assertEquals(unmarshalledCommand, newUnmarshalledCommand,
                "Unmarshalled command should be equal to the original object");
    }

    @Test
    public void testGeneratorDesrialization() throws Exception
    {
        String json = "{'_messageProperties': {'test': 1, 'generator': {'@def': 'list',  '_cyclic': false, '_items': ['first', " +
                "{'@def': 'range', '_upper':10, '_type':'int'}]}}}";
        final TestCommand unmarshalledCommand = _jsonHandler.unmarshall(json, TestCommand.class);

        Map<String, PropertyValue> properties = unmarshalledCommand.getMessageProperties();
        assertNotNull(properties, "Properties should not be null");
        assertFalse(properties.isEmpty(), "Properties should not be empty");
        assertEquals(2, (long) properties.size(), "Unexpected properties size");
        PropertyValue testProperty = properties.get("test");
        assertNotNull(testProperty, "Unexpected property test");
        final boolean condition1 = testProperty.getValue() instanceof Number;
        assertTrue(condition1, "Unexpected property test");
        assertEquals(1, (long) ((Number) testProperty.getValue()).intValue(),
                "Unexpected property value");
        Object generatorObject = properties.get("generator");

        final boolean condition = generatorObject instanceof ListPropertyValue;
        assertTrue(condition, "Unexpected generator object : " + generatorObject);

        PropertyValue generator = (PropertyValue)generatorObject;
        assertEquals("first", generator.getValue(), "Unexpected generator value");
        for (int i = 0; i < 10; i++)
        {
            assertEquals(i, generator.getValue(), "Unexpected generator value");
        }

        String newJson =_jsonHandler.marshall(unmarshalledCommand);
        final TestCommand newUnmarshalledCommand = _jsonHandler.unmarshall(newJson, TestCommand.class);
        assertEquals(unmarshalledCommand, newUnmarshalledCommand,
                "Unmarshalled command should be equal to the original object");
    }

    /**
     * A {@link Command} designed to exercise {@link JsonHandler}, e.g does it handle a map of enums?.
     *
     * This class is non-private to avoid auto-deletion of "unused" fields/methods
     */
    static class SendChristmasCards extends Command
    {
        enum CardType {FUNNY, TRADITIONAL}
        private final Map<CardType, Integer> _cardTypes;
        private List<Person> _persons;

        public SendChristmasCards()
        {
            this(null, null);
        }

        public SendChristmasCards(final CommandType type, Map<CardType, Integer> cardTypes)
        {
            super(type);
            _cardTypes = cardTypes;
        }

        public Map<CardType, Integer> getCardTypes()
        {
            return _cardTypes;
        }

        public List<Person> getPersons()
        {
            return _persons;
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

            final SendChristmasCards that = (SendChristmasCards) o;

            if (getCardTypes() != null ? !getCardTypes().equals(that.getCardTypes()) : that.getCardTypes() != null)
            {
                return false;
            }
            return !(getPersons() != null ? !getPersons().equals(that.getPersons()) : that.getPersons() != null);

        }

        @Override
        public int hashCode()
        {
            int result = getCardTypes() != null ? getCardTypes().hashCode() : 0;
            result = 31 * result + (getPersons() != null ? getPersons().hashCode() : 0);
            return result;
        }
    }

    /**
     * This class is non-private to avoid auto-deletion of "unused" fields/methods
     */
    static class Person
    {
        private final String _firstName;

        public Person()
        {
            this(null);
        }

        public Person(final String firstName)
        {
            _firstName = firstName;
        }

        public String getFirstName()
        {
            return _firstName;
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

            final Person person = (Person) o;

            return !(getFirstName() != null
                    ? !getFirstName().equals(person.getFirstName())
                    : person.getFirstName() != null);

        }

        @Override
        public int hashCode()
        {
            return getFirstName() != null ? getFirstName().hashCode() : 0;
        }
    }

    /**
     * Yet another test class
     */
    static class TestCommand extends Command
    {

        private Map<String, PropertyValue> _messageProperties;

        public TestCommand()
        {
            super(null);
        }

        public TestCommand(CommandType type)
        {
            super(type);
        }

        public Map<String, PropertyValue> getMessageProperties()
        {
            return _messageProperties;
        }

        public void setMessageProperties(Map<String, PropertyValue> _messageProperties)
        {
            this._messageProperties = _messageProperties;
        }

        @Override
        public boolean equals(final Object obj)
        {
            if (obj == null || !(obj instanceof TestCommand))
            {
                return false;
            }
            TestCommand other = (TestCommand)obj;
            if (_messageProperties == null && other._messageProperties != null )
            {
                return false;
            }
            return _messageProperties.equals(other._messageProperties);
        }
    }
}
