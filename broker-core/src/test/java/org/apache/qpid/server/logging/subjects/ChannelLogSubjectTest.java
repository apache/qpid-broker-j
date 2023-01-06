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
package org.apache.qpid.server.logging.subjects;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;

/**
 * Validate ChannelLogSubjects are logged as expected
 */
public class ChannelLogSubjectTest extends ConnectionLogSubjectTest
{

    @BeforeEach
    public void setUp() throws Exception
    {
        super.setUp();
        final int channelID = 1;
        _subject = new ChannelLogSubject(getConnection(), channelID);
    }

    /**
     * MESSAGE [Blank][con:0(MockProtocolSessionUser@null/test)/ch:1] <Log Message>
     *
     * @param message the message whose format needs validation
     */
    @Override
    protected void validateLogStatement(final String message)
    {
        // Use the ConnectionLogSubjectTest to validate that the connection
        // section is ok
        super.validateLogStatement(message);

        // Finally check that the channel identifier is correctly added
        assertTrue(message.contains(")/ch:1]"), "Channel 1 identifier not found as part of Subject");
    }
}
