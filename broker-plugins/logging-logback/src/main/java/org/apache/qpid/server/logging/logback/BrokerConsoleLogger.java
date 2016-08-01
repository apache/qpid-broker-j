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
package org.apache.qpid.server.logging.logback;

import org.apache.qpid.server.model.BrokerLogger;
import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.model.ManagedObject;

@ManagedObject( category = false, type = BrokerConsoleLogger.TYPE, validChildTypes = "org.apache.qpid.server.logging.logback.AbstractLogger#getSupportedBrokerLoggerChildTypes()")
public interface BrokerConsoleLogger<X extends BrokerConsoleLogger<X>> extends BrokerLogger<X>
{
    String TYPE = "Console";

    @ManagedAttribute(defaultValue = "%date %-5level [%thread] \\(%logger{2}\\) - %msg%n")
    String getLayout();

    @ManagedAttribute(defaultValue = "STDOUT", validValues = {"org.apache.qpid.server.logging.logback.BrokerConsoleLoggerImpl#getAllConsoleStreamTarget()"})
    ConsoleStreamTarget getConsoleStreamTarget();

    enum ConsoleStreamTarget
    {
        STDOUT,
        STDERR
    }
}
