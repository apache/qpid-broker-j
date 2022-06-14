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
package org.apache.qpid.server.logging.messages;

import java.util.List;

import org.junit.Test;

import org.apache.qpid.server.logging.Outcome;

/**
 * Test ManagedUser Log Messages
 */
public class UserMessagesTest extends AbstractTestMessages
{
    @Test
    public void managedUserCreated()
    {
        final String name = "test";
        final String attributes = "{\"type\": \"managed\"}";
        _logMessage = UserMessages.CREATE(name, String.valueOf(Outcome.SUCCESS), attributes);
        final List<Object> log = performLog();
        final String[] expected = {"Create : \"", name, "\" : ", String.valueOf(Outcome.SUCCESS), " : ", attributes};
        validateLogMessage(log, "USR-1001", expected);
    }

    @Test
    public void managedUserUpdated()
    {
        final String name = "test";
        final String attributes = "{\"type\": \"managed\"}";
        _logMessage = UserMessages.UPDATE(name, String.valueOf(Outcome.SUCCESS), attributes);
        final List<Object> log = performLog();
        final String[] expected = {"Update : \"", name, "\" : ", String.valueOf(Outcome.SUCCESS), " : ", attributes};
        validateLogMessage(log, "USR-1002", expected);
    }

    @Test
    public void managedUserDeleted()
    {
        final String name = "test";
        _logMessage = UserMessages.DELETE(name, String.valueOf(Outcome.SUCCESS));
        final List<Object> log = performLog();
        final String[] expected = {"Delete : \"", name, "\" : ", String.valueOf(Outcome.SUCCESS)};
        validateLogMessage(log, "USR-1003", expected);
    }
}
