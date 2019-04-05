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
 */

package org.apache.qpid.server.store.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;


public class JDBCDetailsTest extends UnitTestBase{

    @Test
    public void testDerby()
    {
        JDBCDetails derbyDetails = JDBCDetails.getJdbcDetails("derby", Collections.emptyMap());
        assertEquals("derby", derbyDetails.getVendor());
        assertEquals("varchar(%d) for bit data", derbyDetails.getVarBinaryType());
        assertEquals("bigint", derbyDetails.getBigintType());
        assertEquals("blob", derbyDetails.getBlobType());
        assertEquals("", derbyDetails.getBlobStorage());
        assertEquals("timestamp", derbyDetails.getTimestampType());
        assertFalse(derbyDetails.isUseBytesMethodsForBlob());

        assertTrue(derbyDetails.isKnownVendor());
        assertFalse(derbyDetails.isOverridden());
    }

    @Test
    public void testUnknownVendor_UsesFallbackDetails()
    {
        JDBCDetails details = JDBCDetails.getJdbcDetails("homedb", Collections.emptyMap());
        assertEquals("fallback", details.getVendor());
        assertEquals("varchar(%d) for bit data", details.getVarBinaryType());
        assertEquals("bigint", details.getBigintType());
        assertEquals("blob", details.getBlobType());
        assertEquals("", details.getBlobStorage());
        assertEquals("timestamp", details.getTimestampType());
        assertFalse(details.isUseBytesMethodsForBlob());

        assertFalse(details.isOverridden());
        assertFalse(details.isKnownVendor());
    }

    @Test
    public void testDerbyWithOverride()
    {
        Map<String, String> contextMap = new HashMap<>();
        contextMap.put(JDBCDetails.CONTEXT_JDBCSTORE_VARBINARYTYPE, "myvarbin");

        JDBCDetails derbyDetails = JDBCDetails.getJdbcDetails("derby", contextMap);
        assertEquals("derby", derbyDetails.getVendor());
        assertEquals("myvarbin", derbyDetails.getVarBinaryType());
        assertEquals("bigint", derbyDetails.getBigintType());
        assertEquals("blob", derbyDetails.getBlobType());
        assertEquals("", derbyDetails.getBlobStorage());
        assertEquals("timestamp", derbyDetails.getTimestampType());
        assertFalse(derbyDetails.isUseBytesMethodsForBlob());

        assertTrue(derbyDetails.isKnownVendor());
        assertTrue(derbyDetails.isOverridden());
    }

    @Test
    public void testRecognisedDriver_AllDetailsProvidedByContext()
    {
        Map<String, String> contextMap = new HashMap<>();
        contextMap.put(JDBCDetails.CONTEXT_JDBCSTORE_VARBINARYTYPE, "myvarbin");
        contextMap.put(JDBCDetails.CONTEXT_JDBCSTORE_BIGINTTYPE, "mybigint");
        contextMap.put(JDBCDetails.CONTEXT_JDBCSTORE_BLOBTYPE, "myblob");
        contextMap.put(JDBCDetails.CONTEXT_JDBCSTORE_BLOBSTORAGE, "myblobstorage");
        contextMap.put(JDBCDetails.CONTEXT_JDBCSTORE_TIMESTAMPTYPE, "mytimestamp");
        contextMap.put(JDBCDetails.CONTEXT_JDBCSTORE_USEBYTESFORBLOB, "true");

        JDBCDetails details = JDBCDetails.getJdbcDetails("sybase", contextMap);
        assertEquals("sybase", details.getVendor());
        assertEquals("myvarbin", details.getVarBinaryType());
        assertEquals("mybigint", details.getBigintType());
        assertEquals("myblob", details.getBlobType());
        assertEquals("myblobstorage", details.getBlobStorage());
        assertEquals("mytimestamp", details.getTimestampType());
        assertEquals(true, details.isUseBytesMethodsForBlob());

        assertTrue(details.isKnownVendor());
        assertTrue(details.isOverridden());
    }

    @Test
    public void testOracle()
    {
        JDBCDetails oracleDetails = JDBCDetails.getJdbcDetails("oracle", Collections.emptyMap());
        assertEquals("oracle", oracleDetails.getVendor());
        assertEquals("raw(%d)", oracleDetails.getVarBinaryType());
        assertEquals("number", oracleDetails.getBigintType());
        assertEquals("blob", oracleDetails.getBlobType());
        assertEquals("LOB (%s) STORE AS SECUREFILE (RETENTION NONE)", oracleDetails.getBlobStorage());
        assertEquals("timestamp", oracleDetails.getTimestampType());
        assertFalse(oracleDetails.isUseBytesMethodsForBlob());

        assertTrue(oracleDetails.isKnownVendor());
        assertFalse(oracleDetails.isOverridden());
    }
}
