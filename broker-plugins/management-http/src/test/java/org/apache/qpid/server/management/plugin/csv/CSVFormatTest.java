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
package org.apache.qpid.server.management.plugin.csv;

import static org.junit.Assert.assertEquals;

import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class CSVFormatTest extends UnitTestBase
{

    @Test
    public void testPrintRecord() throws Exception
    {
        CSVFormat csvFormat = new CSVFormat();
        final StringWriter out = new StringWriter();
        csvFormat.printRecord(out, Arrays.asList("test", 1, true, "\"quoted\" test"));
        assertEquals("Unexpected format",
                            String.format("%s,%d,%b,%s%s", "test", 1, true, "\"\"\"quoted\"\" test\"", "\r\n"),
                            out.toString());
    }

    @Test
    public void testPrintRecords() throws Exception
    {
        CSVFormat csvFormat = new CSVFormat();
        final StringWriter out = new StringWriter();
        csvFormat.printRecords(out, Arrays.asList(Arrays.asList("test", 1, true, "\"quoted\" test"),
                                                  Arrays.asList("delimeter,test", 1.0f, false,
                                                                "quote\" in the middle")));
        assertEquals("Unexpected format",
                            String.format("%s,%d,%b,%s%s%s,%s,%b,%s%s", "test", 1, true, "\"\"\"quoted\"\" test\"", "\r\n",
                                          "\"delimeter,test\"", "1.0", false, "\"quote\"\" in the middle\"", "\r\n"),
                            out.toString());
    }

    @Test
    public void testPrintln() throws Exception
    {
        CSVFormat csvFormat = new CSVFormat();
        final StringWriter out = new StringWriter();
        csvFormat.println(out);
        assertEquals("Unexpected new line", "\r\n", out.toString());
    }

    @Test
    public void testPrint() throws Exception
    {
        CSVFormat csvFormat = new CSVFormat();
        final StringWriter out = new StringWriter();
        csvFormat.print(out, "test", true);
        csvFormat.print(out, 1, false);
        csvFormat.print(out, true, false);
        csvFormat.print(out, "\"quoted\" test", false);
        assertEquals("Unexpected format ",
                            String.format("%s,%d,%b,%s", "test", 1, true, "\"\"\"quoted\"\" test\""),
                            out.toString());
    }

    @Test
    public void testDate() throws Exception
    {
        Date date = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        CSVFormat csvFormat = new CSVFormat();
        final StringWriter out = new StringWriter();
        csvFormat.print(out, date, true);
        assertEquals("Unexpected format ", simpleDateFormat.format(date), out.toString());
    }

    @Test
    public void testPrintComments() throws Exception
    {
        CSVFormat csvFormat = new CSVFormat();
        final StringWriter out = new StringWriter();
        csvFormat.printComments(out, "comment1", "comment2");
        assertEquals("Unexpected format of comments",
                            String.format("# %s%s# %s%s", "comment1", "\r\n", "comment2", "\r\n"),
                            out.toString());
    }
}
