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
package org.apache.qpid.server.store.berkeleydb.tuple;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.Map;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.UUIDGenerator;
import org.apache.qpid.server.store.ConfiguredObjectRecord;
import org.apache.qpid.server.store.ConfiguredObjectRecordImpl;
import org.apache.qpid.test.utils.UnitTestBase;

public class ConfiguredObjectBindingTest extends UnitTestBase
{

    private ConfiguredObjectRecord _object;

    private static final Map<String, Object> DUMMY_ATTRIBUTES_MAP =
            Collections.singletonMap("dummy", "attributes");

    private static final String DUMMY_TYPE_STRING = "dummyType";
    private ConfiguredObjectBinding _configuredObjectBinding;

    @BeforeEach
    public void setUp() throws Exception
    {
        _configuredObjectBinding = ConfiguredObjectBinding.getInstance();
        _object = new ConfiguredObjectRecordImpl(UUIDGenerator.generateRandomUUID(), DUMMY_TYPE_STRING,
                DUMMY_ATTRIBUTES_MAP);
    }

    @Test
    public void testObjectToEntryAndEntryToObject()
    {
        TupleOutput tupleOutput = new TupleOutput();

        _configuredObjectBinding.objectToEntry(_object, tupleOutput);

        byte[] entryAsBytes = tupleOutput.getBufferBytes();
        TupleInput tupleInput = new TupleInput(entryAsBytes);

        ConfiguredObjectRecord storedObject = _configuredObjectBinding.entryToObject(tupleInput);
        assertEquals(DUMMY_ATTRIBUTES_MAP, storedObject.getAttributes(), "Unexpected attributes");
        assertEquals(DUMMY_TYPE_STRING, storedObject.getType(), "Unexpected type");
    }
}
