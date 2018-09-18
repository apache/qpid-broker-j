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
package org.apache.qpid.server.model;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableSet;
import static org.apache.qpid.server.model.AttributeValueConverter.getConverter;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.text.ParseException;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.model.testmodels.hierarchy.TestCar;
import org.apache.qpid.server.model.testmodels.hierarchy.TestModel;
import org.apache.qpid.test.utils.UnitTestBase;

public class AttributeValueConverterTest extends UnitTestBase
{
    private final ConfiguredObjectFactory _objectFactory = TestModel.getInstance().getObjectFactory();
    private final Map<String, Object> _attributes = new HashMap<>();
    private final Map<String, String> _context = new HashMap<>();

    @Before
    public void setUp() throws Exception
    {

        _attributes.put(ConfiguredObject.NAME, "objectName");
        _attributes.put(ConfiguredObject.CONTEXT, _context);
    }

    @Test
    public void testMapConverter()
    {
        _context.put("simpleMap", "{\"a\" : \"b\"}");
        _context.put("mapWithInterpolatedContents", "{\"${mykey}\" : \"b\"}");
        _context.put("mykey", "mykey1");

        ConfiguredObject object = _objectFactory.create(TestCar.class, _attributes, null);

        AttributeValueConverter<Map> mapConverter = getConverter(Map.class, Map.class);

        Map<String, String> nullMap = mapConverter.convert(null, object);
        assertNull(nullMap);

        Map<String, String> emptyMap = mapConverter.convert("{ }", object);
        assertEquals(emptyMap(), emptyMap);

        Map<String, String> map = mapConverter.convert("{\"a\" : \"b\"}", object);
        assertEquals(singletonMap("a", "b"), map);

        Map<String, String> mapFromInterpolatedVar = mapConverter.convert("${simpleMap}", object);
        assertEquals(singletonMap("a", "b"), mapFromInterpolatedVar);

        Map<String, String> mapFromInterpolatedVarWithInterpolatedContents =
                mapConverter.convert("${mapWithInterpolatedContents}", object);
        assertEquals(singletonMap("mykey1", "b"), mapFromInterpolatedVarWithInterpolatedContents);

        try
        {
            mapConverter.convert("not a map", object);
            fail("Exception not thrown");
        }
        catch (IllegalArgumentException e)
        {
            // PASS
        }
    }

    @Test
    public void testDateConverter() throws Exception
    {
        final long nowMillis = System.currentTimeMillis();
        final Date now = new Date(nowMillis);

        ConfiguredObject object = _objectFactory.create(TestCar.class, _attributes, null);

        AttributeValueConverter<Date> converter = getConverter(Date.class, Date.class);

        assertNull("Cannot convert null", converter.convert(null, object));

        assertEquals("Cannot convert date expressed as Date", now, converter.convert(now, object));

        assertEquals("Cannot convert date expressed as Number",
                            new Date(nowMillis),
                            converter.convert(nowMillis, object));

        assertEquals("Cannot convert date expressed as String containing Number",
                            new Date(nowMillis),
                            converter.convert("" + nowMillis, object));

        final String iso8601DateTime = "1970-01-01T00:00:01Z";
        assertEquals("Cannot convert date expressed as ISO8601 date time",
                            new Date(1000),
                            converter.convert(iso8601DateTime, object));

        final String iso8601Date = "1970-01-02";
        assertEquals("Cannot convert date expressed as ISO8601 date",
                            new Date(TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS)),
                            converter.convert(iso8601Date, object));

        try
        {
            converter.convert("elephant", object);
            fail("Exception not thrown");
        }
        catch (IllegalArgumentException e)
        {
            // PASS
        }

    }

    @Test
    public void testNonGenericCollectionConverter()
    {
        _context.put("simpleCollection", "[\"a\", \"b\"]");

        ConfiguredObject object = _objectFactory.create(TestCar.class, _attributes, null);

        AttributeValueConverter<Collection> collectionConverter = getConverter(Collection.class, Collection.class);

        Collection<String> nullCollection = collectionConverter.convert(null, object);
        assertNull(nullCollection);

        Collection<String> emptyCollection = collectionConverter.convert("[ ]", object);
        assertTrue(emptyCollection.isEmpty());

        Collection<String> collection = collectionConverter.convert("[\"a\",  \"b\"]", object);
        assertEquals((long) 2, (long) collection.size());
        assertTrue(collection.contains("a"));
        assertTrue(collection.contains("b"));

        Collection<String> collectionFromInterpolatedVar = collectionConverter.convert("${simpleCollection}", object);
        assertEquals((long) 2, (long) collectionFromInterpolatedVar.size());
        assertTrue(collectionFromInterpolatedVar.contains("a"));
        assertTrue(collectionFromInterpolatedVar.contains("b"));

        try
        {
            collectionConverter.convert("not a collection", object);
            fail("Exception not thrown");
        }
        catch (IllegalArgumentException e)
        {
            // PASS
        }
    }

    @Test
    public void testNonGenericListConverter()
    {
        _context.put("simpleList", "[\"a\", \"b\"]");

        ConfiguredObject object = _objectFactory.create(TestCar.class, _attributes, null);

        AttributeValueConverter<List> listConverter = getConverter(List.class, List.class);

        List<String> nullList = listConverter.convert(null, object);
        assertNull(nullList);

        List<String> emptyList = listConverter.convert("[ ]", object);
        assertTrue(emptyList.isEmpty());

        List<String> expectedList = unmodifiableList(asList("a", "b"));

        List<String> list = listConverter.convert("[\"a\",  \"b\"]", object);
        assertEquals(expectedList, list);

        List<String> listFromInterpolatedVar = listConverter.convert("${simpleList}", object);
        assertEquals(expectedList, listFromInterpolatedVar);

        try
        {
            listConverter.convert("not a list", object);
            fail("Exception not thrown");
        }
        catch (IllegalArgumentException e)
        {
            // PASS
        }
    }

    @Test
    public void testNonGenericSetConverter()
    {
        _context.put("simpleSet", "[\"a\", \"b\"]");

        ConfiguredObject object = _objectFactory.create(TestCar.class, _attributes, null);

        AttributeValueConverter<Set> setConverter = getConverter(Set.class, Set.class);;

        Set<String> nullSet = setConverter.convert(null, object);
        assertNull(nullSet);

        Set<String> emptySet = setConverter.convert("[ ]", object);
        assertTrue(emptySet.isEmpty());

        Set<String> expectedSet = unmodifiableSet(new HashSet<>(asList("a", "b")));

        Set<String> set = setConverter.convert("[\"a\",  \"b\"]", object);
        assertEquals(expectedSet, set);

        Set<String> setFromInterpolatedVar = setConverter.convert("${simpleSet}", object);
        assertEquals(expectedSet, setFromInterpolatedVar);

        try
        {
            setConverter.convert("not a set", object);
            fail("Exception not thrown");
        }
        catch (IllegalArgumentException e)
        {
            // PASS
        }
    }

    private static String BASE_64_ENCODED_CERTIFICATE = "MIIC4TCCAkqgAwIBAgIFAKI1xCswDQYJKoZIhvcNAQEFBQAwQTELMAkGA1UE"
                                                        + "BhMCQ0ExEDAOBgNVBAgTB09udGFyaW8xDTALBgNVBAoTBEFDTUUxETAPBg"
                                                        + "NVBAMTCE15Um9vdENBMB4XDTE1MDMyMDAxMjEwNVoXDTIwMDMyMDAxMjEw"
                                                        + "NVowYTELMAkGA1UEBhMCQ0ExCzAJBgNVBAgTAk9OMRAwDgYDVQQHEwdUb3"
                                                        + "JvbnRvMQ0wCwYDVQQKEwRhY21lMQwwCgYDVQQLEwNhcnQxFjAUBgNVBAMM"
                                                        + "DWFwcDJAYWNtZS5vcmcwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAo"
                                                        + "IBAQCviLTH6Vl6gP3M6gmmm0sVlCcBFfo2czDTsr93D1cIQpnyY1r3znBd"
                                                        + "FT3cbXE2LtHeLpnlXc+dTo9/aoUuBCzRIpi4CeaGgD3ggIl9Ws5hUgfxJC"
                                                        + "WBg7nhzMUlBC2C+VgIUHWHqGPuaQ7VzXOEC7xF0mihMZ4bwvU6wxGK2uUo"
                                                        + "ruXE/iti/+jtzxjq0PO7ZgJ7GUI2ZDqGMad5OnLur8jz+yKsVdetXlXsOy"
                                                        + "HmHi/47pRuA115pYiIaZKu1+vs6IBl4HnEUgw5JwIww6oyTDVvXc1kCw0Q"
                                                        + "CtUZMcNSH2XGhh/zGM/M2Bt2lgEEW0xWTwQcT1J7wnngfbIYbzoupEkRAg"
                                                        + "MBAAGjQTA/MB0GA1UdDgQWBBRI+VUMRkfNYp/xngM9y720hvxmXTAJBgNV"
                                                        + "HRMEAjAAMBMGA1UdJQQMMAoGCCsGAQUFBwMCMA0GCSqGSIb3DQEBBQUAA4"
                                                        + "GBAJnedohhbqoY7O6oAm+hPScBCng/fl0erVjexL9W8l8g5NvIGgioUfjU"
                                                        + "DvGOnwB5LOoTnZUCRaLFhQFcGFMIjdHpg0qt/QkEFX/0m+849RK6muHT1C"
                                                        + "NlcXtCFXwPTJ+9h+1auTP+Yp/6ii9SU3W1dzYawy2p9IhkMZEpJaHCLnaC";

    @Test
    public void testBase64EncodedCertificateConverter() throws ParseException
    {
        ConfiguredObject object = _objectFactory.create(TestCar.class, _attributes, null);
        AttributeValueConverter<Certificate> certificateConverter = getConverter(Certificate.class, Certificate.class);
        Certificate certificate = certificateConverter.convert(BASE_64_ENCODED_CERTIFICATE, object);
        final boolean condition = certificate instanceof X509Certificate;
        assertTrue("Unexpected certificate", condition);
        X509Certificate x509Certificate = (X509Certificate)certificate;
        assertEquals("CN=app2@acme.org,OU=art,O=acme,L=Toronto,ST=ON,C=CA",
                            x509Certificate.getSubjectX500Principal().getName());

        assertEquals("CN=MyRootCA,O=ACME,ST=Ontario,C=CA", x509Certificate.getIssuerX500Principal().getName());
    }

    @Test
    public void testPEMCertificateConverter() throws ParseException
    {
        ConfiguredObject object = _objectFactory.create(TestCar.class, _attributes, null);
        AttributeValueConverter<Certificate> certificateConverter = getConverter(Certificate.class, Certificate.class);
        StringBuffer pemCertificate = new StringBuffer("-----BEGIN CERTIFICATE-----\n");
        int offset = 0;
        while(BASE_64_ENCODED_CERTIFICATE.length() - offset > 64)
        {
            pemCertificate.append(BASE_64_ENCODED_CERTIFICATE.substring(offset, offset + 64)).append('\n');
            offset += 64;
        }
        pemCertificate.append(BASE_64_ENCODED_CERTIFICATE.substring(offset));
        pemCertificate.append("\n-----END CERTIFICATE-----\n");

        Certificate certificate = certificateConverter.convert(pemCertificate.toString(), object);
        final boolean condition = certificate instanceof X509Certificate;
        assertTrue("Unexpected certificate", condition);
        X509Certificate x509Certificate = (X509Certificate)certificate;
        assertEquals("CN=app2@acme.org,OU=art,O=acme,L=Toronto,ST=ON,C=CA",
                            x509Certificate.getSubjectX500Principal().getName());
        assertEquals("CN=MyRootCA,O=ACME,ST=Ontario,C=CA", x509Certificate.getIssuerX500Principal().getName());
    }

    @Test
    public void testMapToManagedAttributeValue()
    {
        ConfiguredObject object = _objectFactory.create(TestCar.class, _attributes, null);

        final AttributeValueConverter<TestManagedAttributeValue> converter =
                getConverter(TestManagedAttributeValue.class, TestManagedAttributeValue.class);

        final String expectedStringValue = "mystringvalue";
        final Integer expectedIntegerValue = 31;
        final int expectedIntegerPrimitiveValue = 32;
        final Map<String, Object> input = new HashMap<>();
        input.put("string", expectedStringValue);
        input.put("integer", expectedIntegerValue);
        input.put("int", expectedIntegerPrimitiveValue);

        final TestManagedAttributeValue value = converter.convert(input, object);

        assertEquals(expectedStringValue, value.getString());
        assertEquals(expectedIntegerValue, value.getInteger());
        assertEquals((long) expectedIntegerPrimitiveValue, (long) value.getInt());
        assertNull(expectedStringValue, value.getAnotherString());

        final TestManagedAttributeValue nullValues = converter.convert(Collections.emptyMap(), object);

        assertNull(nullValues.getString());
        assertNull(nullValues.getInteger());
        assertEquals((long) 0, (long) nullValues.getInt());
        assertNull(expectedStringValue, nullValues.getAnotherString());
    }

    @ManagedAttributeValueType
    public interface TestManagedAttributeValue extends ManagedAttributeValue
    {
        String getString();
        Integer getInteger();
        int getInt();
        String getAnotherString();
    }

    @Test
    public void testMapToManagedAttributeValueEquality()
    {
        ConfiguredObject object = _objectFactory.create(TestCar.class, _attributes, null);

        final AttributeValueConverter<SimpleTestManagedAttributeValue> converter =
                getConverter(SimpleTestManagedAttributeValue.class, SimpleTestManagedAttributeValue.class);

        Object elephant = new Object();

        final Map<String, String> map = Collections.singletonMap("string", "mystring");
        final Map<String, String> mapWithSameContent = Collections.singletonMap("string", "mystring");
        final Map<String, String> mapWithDifferentContent = Collections.singletonMap("string", "mydifferentstring");

        final SimpleTestManagedAttributeValue value = converter.convert(map, object);
        final SimpleTestManagedAttributeValue same = converter.convert(map, object);
        final SimpleTestManagedAttributeValue sameContent = converter.convert(mapWithSameContent, object);
        final SimpleTestManagedAttributeValue differentContent = converter.convert(mapWithDifferentContent, object);

        assertFalse(value.equals(elephant));
        assertTrue(value.equals(value));
        assertTrue(value.equals(same));
        assertTrue(sameContent.equals(value));
        assertFalse(differentContent.equals(value));
    }

    @ManagedAttributeValueType
    public interface SimpleTestManagedAttributeValue extends ManagedAttributeValue
    {
        String getString();
    }

    @Test
    public void testMapToManagedAttributeValueWithFactory()
    {
        ConfiguredObject object = _objectFactory.create(TestCar.class, _attributes, null);

        final AttributeValueConverter<SimpleTestManagedAttributeValueWithFactory> converter =
                getConverter(SimpleTestManagedAttributeValueWithFactory.class, SimpleTestManagedAttributeValueWithFactory.class);

        Object elephant = new Object();

        final Map<String, String> map = Collections.singletonMap("string", "mystring");

        final SimpleTestManagedAttributeValueWithFactory value = converter.convert(map, object);

        assertTrue(value.getClass().equals(SimpleTestManagedAttributeValueWithFactoryImpl.class));
        assertTrue(value.getString().equals("mystring"));
    }


    @ManagedAttributeValueType
    public interface SimpleTestManagedAttributeValueWithFactory extends ManagedAttributeValue
    {
        String getString();

        @ManagedAttributeValueTypeFactoryMethod
        static SimpleTestManagedAttributeValueWithFactory newInstance(SimpleTestManagedAttributeValueWithFactory instance)
        {
            return new SimpleTestManagedAttributeValueWithFactoryImpl(instance.getString());
        }

    }

    static class SimpleTestManagedAttributeValueWithFactoryImpl implements SimpleTestManagedAttributeValueWithFactory
    {
        private final String _string;

        public SimpleTestManagedAttributeValueWithFactoryImpl(
                final String string)
        {
            _string = string;
        }

        @Override
        public String getString()
        {
            return _string;
        }
    }
}
