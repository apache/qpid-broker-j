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

import java.util.List;
import java.util.Set;

import org.apache.qpid.server.logging.LogFileDetails;
import org.apache.qpid.server.model.BrokerLogger;
import org.apache.qpid.server.model.DerivedAttribute;
import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.model.ManagedContextDefault;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedOperation;
import org.apache.qpid.server.model.Param;
import org.apache.qpid.server.model.Content;

@ManagedObject( category = false, type = BrokerFileLogger.TYPE, validChildTypes = "org.apache.qpid.server.logging.logback.AbstractLogger#getSupportedBrokerLoggerChildTypes()")
public interface BrokerFileLogger<X extends BrokerFileLogger<X>> extends BrokerLogger<X>
{
    String TYPE = "File";
    String FILE_NAME = "fileName";
    String MAX_FILE_SIZE = "maxFileSize";

    String BROKER_FAIL_ON_LOGGER_IO_ERROR = "broker.failOnLoggerIOError";
    @ManagedContextDefault(name = BROKER_FAIL_ON_LOGGER_IO_ERROR)
    String DEFAULT_BROKER_FAIL_ON_LOGGER_IO_ERROR = "false";

    @ManagedAttribute( defaultValue = "${qpid.work_dir}${file.separator}log${file.separator}qpid.log")
    String getFileName();

    @ManagedAttribute( defaultValue = "false")
    boolean isRollDaily();

    @ManagedAttribute( defaultValue = "false")
    boolean isRollOnRestart();

    @ManagedAttribute( defaultValue = "false")
    boolean isCompressOldFiles();

    @ManagedAttribute( defaultValue = "1")
    int getMaxHistory();

    @ManagedAttribute( defaultValue = "100")
    int getMaxFileSize();

    @ManagedAttribute(defaultValue = "%date %-5level [%thread] \\(%logger{2}\\) - %msg%n")
    String getLayout();

    @DerivedAttribute
    List<LogFileDetails> getLogFiles();

    @ManagedOperation(nonModifying = true, changesConfiguredObjectState = false)
    Content getFile(@Param(name = "fileName", mandatory = true) String fileName);

    @ManagedOperation(nonModifying = true, changesConfiguredObjectState = false)
    Content getFiles(@Param(name = "fileName", mandatory = true) Set<String> fileName);

    @ManagedOperation(nonModifying = true,
            changesConfiguredObjectState = false)
    Content getAllFiles();
}
