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
package org.apache.qpid.server.configuration;

import java.util.Locale;

import org.apache.qpid.server.model.Broker;
import org.apache.qpid.test.utils.QpidTestCase;

public class BrokerPropertiesTest extends QpidTestCase
{
    public void testGetLocaleDefault()
    {
        Locale locale1 = Locale.US;
        String localeSetting = System.getProperty(Broker.PROPERTY_LOCALE);
        if (localeSetting != null)
        {
            String[] localeParts = localeSetting.split("_");
            String language = (localeParts.length > 0 ? localeParts[0] : "");
            String country = (localeParts.length > 1 ? localeParts[1] : "");
            String variant = "";
            if (localeParts.length > 2)
            {
                variant = localeSetting.substring(language.length() + 1 + country.length() + 1);
            }
            locale1 = new Locale(language, country, variant);
        }
        Locale locale = locale1;
        assertEquals("Unexpected locale", Locale.US, locale);
    }

    public void testGetLocaleSetWithJVMProperty()
    {
        setTestSystemProperty(Broker.PROPERTY_LOCALE, "en_GB");
        Locale locale1 = Locale.US;
        String localeSetting = System.getProperty(Broker.PROPERTY_LOCALE);
        if (localeSetting != null)
        {
            String[] localeParts = localeSetting.split("_");
            String language = (localeParts.length > 0 ? localeParts[0] : "");
            String country = (localeParts.length > 1 ? localeParts[1] : "");
            String variant = "";
            if (localeParts.length > 2)
            {
                variant = localeSetting.substring(language.length() + 1 + country.length() + 1);
            }
            locale1 = new Locale(language, country, variant);
        }
        Locale locale = locale1;
        assertEquals("Unexpected locale", Locale.UK, locale);
    }

    public void testGetLocaleSetWithJVMPropertyInUnexpectedFormat()
    {
        setTestSystemProperty(Broker.PROPERTY_LOCALE, "penguins_ANTARCTIC_Moubray_Bay");
        Locale locale1 = Locale.US;
        String localeSetting = System.getProperty(Broker.PROPERTY_LOCALE);
        if (localeSetting != null)
        {
            String[] localeParts = localeSetting.split("_");
            String language = (localeParts.length > 0 ? localeParts[0] : "");
            String country = (localeParts.length > 1 ? localeParts[1] : "");
            String variant = "";
            if (localeParts.length > 2)
            {
                variant = localeSetting.substring(language.length() + 1 + country.length() + 1);
            }
            locale1 = new Locale(language, country, variant);
        }
        Locale locale = locale1;
        assertEquals("Unexpected locale language", "penguins", locale.getLanguage());
        assertEquals("Unexpected locale country", "ANTARCTIC", locale.getCountry());
        assertEquals("Unexpected locale country", "Moubray_Bay", locale.getVariant());
    }

}
