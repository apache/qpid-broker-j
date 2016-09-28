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

import java.util.Collection;

import org.apache.qpid.server.model.BrokerLogger;
import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.model.ManagedContextDefault;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedOperation;
import org.apache.qpid.server.model.Param;

@ManagedObject( category = false, type = BrokerMemoryLogger.TYPE,
                validChildTypes = "org.apache.qpid.server.logging.logback.AbstractLogger#getSupportedBrokerLoggerChildTypes()")
public interface BrokerMemoryLogger<X extends BrokerMemoryLogger<X>> extends BrokerLogger<X>
{
    String MAX_RECORDS = "maxRecords";

    String TYPE = "Memory";

    String BROKERMEMORYLOGGER_MAX_RECORD_LIMIT_VAR  = "brokermemorylogger.max_record_limit";

    @ManagedContextDefault(name = BROKERMEMORYLOGGER_MAX_RECORD_LIMIT_VAR)
    int MAX_RECORD_LIMIT = 16384;

    @ManagedAttribute( defaultValue = "4096" )
    int getMaxRecords();

    @ManagedOperation(nonModifying = true, changesConfiguredObjectState = false)
    Collection<LogRecord> getLogEntries(@Param(name="lastLogId", defaultValue="0") long lastLogId);

}
