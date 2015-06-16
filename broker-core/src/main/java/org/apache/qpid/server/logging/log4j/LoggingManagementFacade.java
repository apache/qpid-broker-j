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
package org.apache.qpid.server.logging.log4j;


import java.util.List;
import java.util.Map;


public class LoggingManagementFacade
{
    private static transient LoggingManagementFacade _instance;

    public static LoggingManagementFacade configure(String filename) throws LoggingFacadeException
    {
        _instance = new LoggingManagementFacade(filename);
        return _instance;
    }

    public static LoggingManagementFacade configureAndWatch(String filename, int delay) throws LoggingFacadeException
    {
        _instance = new LoggingManagementFacade(filename, delay);
        return _instance;
    }

    public static LoggingManagementFacade getCurrentInstance()
    {
        return _instance;
    }

    private LoggingManagementFacade(String filename)
    {

    }

    private LoggingManagementFacade(String filename, int delay)
    {
        throw new UnsupportedOperationException();
    }

    public int getLog4jLogWatchInterval()
    {
        throw new UnsupportedOperationException();
    }

    public synchronized void reload() throws LoggingFacadeException
    {
        throw new UnsupportedOperationException();
    }

    /** The log4j XML configuration file DTD defines three possible element
     * combinations for specifying optional logger+level settings.
     * Must account for the following:
     * <p>
     * {@literal <category name="x"> <priority value="y"/> </category>} OR
     * {@literal <category name="x"> <level value="y"/> </category>} OR
     * {@literal <logger name="x"> <level value="y"/> </logger>}
     * <p>
     * Noting also that the level/priority child element is optional too,
     * and not the only possible child element.
     */
    public synchronized Map<String,String> retrieveConfigFileLoggersLevels() throws LoggingFacadeException
    {
        throw new UnsupportedOperationException();
    }

    /**
     * The log4j XML configuration file DTD defines 2 possible element
     * combinations for specifying the optional root logger level settings
     * Must account for the following:
     * <p>
     * {@literal <root> <priority value="y"/> </root> } OR
     * {@literal <root> <level value="y"/> </root>}
     * <p>
     * Noting also that the level/priority child element is optional too,
     * and not the only possible child element.
     */
    public synchronized String retrieveConfigFileRootLoggerLevel() throws LoggingFacadeException
    {
        throw new UnsupportedOperationException();
    }

    public synchronized void setConfigFileLoggerLevel(String logger, String level) throws LoggingFacadeException
    {
        throw new UnsupportedOperationException();
    }

    public synchronized void setConfigFileRootLoggerLevel(String level) throws LoggingFacadeException
    {
        throw new UnsupportedOperationException();
    }

    public List<String> getAvailableLoggerLevels()
    {
        throw new UnsupportedOperationException();
    }

    public String retrieveRuntimeRootLoggerLevel()
    {
        throw new UnsupportedOperationException();
    }

    public void setRuntimeRootLoggerLevel(String level)
    {
        throw new UnsupportedOperationException();
    }

    public void setRuntimeLoggerLevel(String loggerName, String level) throws LoggingFacadeException
    {
        throw new UnsupportedOperationException();
    }

    public Map<String,String> retrieveRuntimeLoggersLevels()
    {
        throw new UnsupportedOperationException();
    }

}

