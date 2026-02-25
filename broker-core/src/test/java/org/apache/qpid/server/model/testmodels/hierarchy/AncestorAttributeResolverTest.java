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

package org.apache.qpid.server.model.testmodels.hierarchy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import tools.jackson.databind.ObjectMapper;

import org.apache.qpid.server.model.AncestorAttributeResolver;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.test.utils.UnitTestBase;

public class AncestorAttributeResolverTest extends UnitTestBase
{
    public static final String CAR_NAME = "myCar";
    private final Model _model = TestModel.getInstance();

    private AncestorAttributeResolver _ancestorAttributeResolver;
    private TestCar<?> _car;
    private TestEngine<?> _engine;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception
    {
        final Map<String, Object> carAttributes = Map.of(ConfiguredObject.NAME, CAR_NAME,
                ConfiguredObject.TYPE, TestKitCarImpl.TEST_KITCAR_TYPE);

        _car = _model.getObjectFactory().create(TestCar.class, carAttributes, null);

        assertEquals(CAR_NAME, _car.getName());

        final String engineName = "myEngine";
        final Map<String, Object> engineAttributes = Map.of(ConfiguredObject.NAME, engineName,
                ConfiguredObject.TYPE, TestElecEngineImpl.TEST_ELEC_ENGINE_TYPE);

        _engine = (TestEngine<?>) _car.createChild(TestEngine.class, engineAttributes);
    }

    @Test
    public void testResolveToParent()
    {
        _ancestorAttributeResolver = new AncestorAttributeResolver(_engine);
        final String actual = _ancestorAttributeResolver.resolve("ancestor:testcar:name", null);
        assertEquals(CAR_NAME, actual);
    }

    @Test
    public void testResolveToSelf()
    {
        _ancestorAttributeResolver = new AncestorAttributeResolver(_car);
        final String actual = _ancestorAttributeResolver.resolve("ancestor:testcar:name", null);
        assertEquals(CAR_NAME, actual);
    }

    @Test
    public void testUnrecognisedCategoryName()
    {
        _ancestorAttributeResolver = new AncestorAttributeResolver(_car);
        final String actual = _ancestorAttributeResolver.resolve("ancestor:notacategoty:name", null);
        assertNull(actual);
    }

    @Test
    public void testUnrecognisedAttributeName()
    {
        _ancestorAttributeResolver = new AncestorAttributeResolver(_car);
        final String actual = _ancestorAttributeResolver.resolve("ancestor:notacategoty:nonexisting", null);
        assertNull(actual);
    }

    @Test
    public void testBadAncestorRef()
    {
        _ancestorAttributeResolver = new AncestorAttributeResolver(_car);
        final String actual = _ancestorAttributeResolver.resolve("ancestor:name", null);
        assertNull(actual);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testResolveAncestorAttributeOfTypeMap() throws Exception
    {
        final Map<String, Object> parameters = Map.of("int", 1, "string", "value");
        final Map<String, Object> carAttributes = Map.of(ConfiguredObject.NAME, CAR_NAME,
                ConfiguredObject.TYPE, TestKitCarImpl.TEST_KITCAR_TYPE,
                "parameters", parameters);

        _car = _model.getObjectFactory().create(TestCar.class, carAttributes, null);

        _ancestorAttributeResolver = new AncestorAttributeResolver(_car);
        final String actual = _ancestorAttributeResolver.resolve("ancestor:testcar:parameters", null);

        final ObjectMapper objectMapper = new ObjectMapper();
        final Map<Object,Object> data = objectMapper.readValue(actual, HashMap.class);
        assertEquals(parameters, data, "Unexpected resolved ancestor attribute of type Map");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testResolveAncestorAttributeOfTypeConfiguredObject()
    {
        final Map<String, Object> carAttributes = Map.of(ConfiguredObject.NAME, CAR_NAME,
                ConfiguredObject.TYPE, TestKitCarImpl.TEST_KITCAR_TYPE,
                "alternateEngine", _engine);

        _car = _model.getObjectFactory().create(TestCar.class, carAttributes, null);

        _ancestorAttributeResolver = new AncestorAttributeResolver(_car);
        final String actual = _ancestorAttributeResolver.resolve("ancestor:testcar:alternateEngine", null);

        assertEquals(_engine.getId().toString(), actual,
                "Unexpected resolved ancestor attribute of type ConfiguredObject");
    }
}
