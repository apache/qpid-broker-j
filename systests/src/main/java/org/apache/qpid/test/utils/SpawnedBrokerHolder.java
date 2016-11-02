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
package org.apache.qpid.test.utils;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.BrokerProperties;
import org.apache.qpid.server.logging.logback.BrokerLogbackSocketLogger;
import org.apache.qpid.server.logging.logback.BrokerNameAndLevelLogInclusionRule;
import org.apache.qpid.server.logging.messages.BrokerMessages;
import org.apache.qpid.server.model.BrokerLogInclusionRule;
import org.apache.qpid.server.model.BrokerLogger;
import org.apache.qpid.util.SystemUtils;

public class SpawnedBrokerHolder extends AbstractBrokerHolder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SpawnedBrokerHolder.class);
    protected static final String BROKER_READY = System.getProperty("broker.ready", BrokerMessages.READY().toString());
    private static final String BROKER_STOPPED = System.getProperty("broker.stopped", BrokerMessages.STOPPED().toString());
    private static final String BROKER_COMMAND_PLATFORM = "broker.command." + SystemUtils.getOSConfigSuffix();
    private static final String BROKER_COMMAND_TEMPLATE = System.getProperty(BROKER_COMMAND_PLATFORM, System.getProperty("broker.command"));
    private static final int BROKER_STARTUP_TIME = Integer.parseInt(System.getProperty("SpawnedBrokerHolder.brokerStartupTime", "30000"));

    private final Map<String, String> _jvmOptions;
    private final Map<String, String> _environmentSettings;
    protected BrokerCommandHelper _brokerCommandHelper;

    private  Process _process;
    private  Integer _pid;
    private List<String> _windowsPids;
    private String _brokerCommand;
    private String _pseudoThreadName;

    public SpawnedBrokerHolder(final int port,
                               final String classQualifiedTestName,
                               final File logFile,
                               Map<String, String> jvmOptions,
                               Map<String, String> environmentSettings)
    {
        super(port, classQualifiedTestName, logFile);
        _jvmOptions = jvmOptions;
        _environmentSettings = environmentSettings;
        _brokerCommandHelper = new BrokerCommandHelper(BROKER_COMMAND_TEMPLATE);
        _pseudoThreadName = "BROKER-" + getBrokerIndex();
    }

    @Override
    public void start(final boolean managementMode, final int amqpPort) throws Exception
    {
        Map<String, String> mdc = new HashMap<>();
        mdc.put(QpidBrokerTestCase.CLASS_QUALIFIED_TEST_NAME, getClassQualifiedTestName());
        mdc.put("origin", getLogPrefix());

        LOGGER.debug("Spawning broker with jvmOptions: {} environmentSettings: {} permitted start-up time: {}",
                     _jvmOptions, _environmentSettings, BROKER_STARTUP_TIME);

        String[] cmd = _brokerCommandHelper.getBrokerCommand(amqpPort,
                                                             getWorkDir().toString(),
                                                             getConfigurationPath(),
                                                             getBrokerStoreType());
        if (managementMode)
        {
            String[] newCmd = new String[cmd.length + 3];
            System.arraycopy(cmd, 0, newCmd, 0, cmd.length);
            newCmd[cmd.length] = "-mm";
            newCmd[cmd.length + 1] = "-mmpass";
            newCmd[cmd.length + 2] = QpidBrokerTestCase.MANAGEMENT_MODE_PASSWORD;
            cmd = newCmd;
        }

        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        Map<String, String> processEnv = pb.environment();
        String qpidHome = System.getProperty(BrokerProperties.PROPERTY_QPID_HOME);
        processEnv.put(BrokerProperties.PROPERTY_QPID_HOME, qpidHome);

        //Augment Path with bin directory in QPID_HOME.
        boolean foundPath = false;
        final String pathEntry = qpidHome + File.separator + "bin";
        for(Map.Entry<String,String> entry : processEnv.entrySet())
        {
            if(entry.getKey().equalsIgnoreCase("path"))
            {
                entry.setValue(entry.getValue().concat(File.pathSeparator + pathEntry));
                foundPath = true;
            }
        }
        if(!foundPath)
        {
            processEnv.put("PATH", pathEntry);
        }
        //Add the test name to the broker run.
        // DON'T change PNAME, qpid.stop needs this value.
        processEnv.put("QPID_PNAME", "-DPNAME=QPBRKR -DTNAME=\"" + getClassQualifiedTestName() + "\"");

        // Add all the environment settings the test requested
        if (!_environmentSettings.isEmpty())
        {
            for (Map.Entry<String, String> entry : _environmentSettings.entrySet())
            {
                processEnv.put(entry.getKey(), entry.getValue());
            }
        }

        String qpidOpts = "";

        // Add all the specified system properties to QPID_OPTS
        if (!_jvmOptions.isEmpty())
        {
            boolean isWindows = SystemUtils.isWindows();
            for (String key : _jvmOptions.keySet())
            {
                qpidOpts += " -D" + key + "=" +(isWindows ? doWindowsCommandEscaping(_jvmOptions.get(key)) : _jvmOptions.get(key));
            }
        }

        if (processEnv.containsKey("QPID_OPTS"))
        {
            qpidOpts = processEnv.get("QPID_OPTS") + qpidOpts;
        }
        processEnv.put("QPID_OPTS", qpidOpts);

        _process = pb.start();

        Piper standardOutputPiper = new Piper(_process.getInputStream(),
                BROKER_READY,
                BROKER_STOPPED,
                _pseudoThreadName, getClass().getName());

        standardOutputPiper.start();

        StringBuilder cmdLine = new StringBuilder(cmd[0]);
        for(int i = 1; i< cmd.length; i++)
        {
            cmdLine.append(' ');
            cmdLine.append(cmd[i]);
        }

        _brokerCommand = cmdLine.toString();
        _pid = retrieveUnixPidIfPossible();

        if (!standardOutputPiper.await(BROKER_STARTUP_TIME, TimeUnit.MILLISECONDS))
        {
            LOGGER.info("Spawned broker failed to become ready within {} ms."
                        + " Ready line '{}'",
                        BROKER_STARTUP_TIME, standardOutputPiper.getReady());
            String threadDump = dumpThreads();
            if (!threadDump.isEmpty())
            {
                LOGGER.info("the result of a try to capture thread dump:" + threadDump);
            }
            //Ensure broker has stopped
            _process.destroy();
            throw new RuntimeException("broker failed to become ready:"
                    + standardOutputPiper.getStopLine());
        }

        _windowsPids = retrieveWindowsPidsIfPossible();

        try
        {
            //test that the broker is still running and hasn't exited unexpectedly
            int exit = _process.exitValue();
            LOGGER.info("broker aborted: {}", exit);
            throw new RuntimeException("broker aborted: " + exit);
        }
        catch (IllegalThreadStateException e)
        {
            // this is expect if the broker started successfully
        }

    }

    private String doWindowsCommandEscaping(String value)
    {
        if(value.contains("\"") && !value.startsWith("\""))
        {
        return "\"" + value.replaceAll("\"", "\"\"") + "\"";

        }
        else
        {
            return value;
        }
    }

    @Override
    public void shutdown()
    {
        if(SystemUtils.isWindows())
        {
            doWindowsKill();
        }

        if (_process != null)
        {
            LOGGER.info("Destroying broker process");
            _process.destroy();

            reapChildProcess();
            waitUntilPortsAreFreeIfRequired();
        }
    }

    @Override
    protected String getLogPrefix()
    {
        return _pseudoThreadName;
    }

    private List<String> retrieveWindowsPidsIfPossible()
    {
        if(SystemUtils.isWindows())
        {
            try
            {
                Process p = Runtime.getRuntime().exec(new String[]{"wmic", "process", "list"});
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream())))
                {
                    String line;
                    String headers = reader.readLine();
                    int processIdOffset = headers.indexOf(" ProcessId") + 1;
                    int parentProcessIdOffset = headers.indexOf(" ParentProcessId") + 1;
                    String parentProcess = null;
                    Map<String, List<String>> parentProcessMap = new HashMap<String, List<String>>();

                    while ((line = reader.readLine()) != null)
                    {
                        if (line.length() > processIdOffset)
                        {
                            String processIdStr = line.substring(processIdOffset);
                            processIdStr = processIdStr.substring(0, processIdStr.indexOf(' '));
                            processIdStr = processIdStr.trim();

                            String parentProcessIdStr = line.substring(parentProcessIdOffset);
                            parentProcessIdStr = parentProcessIdStr.substring(0, parentProcessIdStr.indexOf(' '));
                            parentProcessIdStr = parentProcessIdStr.trim();
                            if (parentProcessIdStr.length() > 0 && (parentProcess == null || parentProcess.equals(
                                    parentProcessIdStr)))
                            {
                                List<String> children = parentProcessMap.get(parentProcessIdStr);
                                if (children == null)
                                {
                                    children = new ArrayList<String>();
                                    parentProcessMap.put(parentProcessIdStr, children);
                                }
                                children.add(processIdStr);
                            }
                            if (line.toLowerCase()
                                    .contains(_brokerCommand.toLowerCase()))
                            {
                                parentProcess = processIdStr;
                            }

                        }

                    }
                    LOGGER.debug("Parent process: " + parentProcess);
                    if (parentProcess != null)
                    {
                        List<String> returnVal = new ArrayList<>();
                        returnVal.add(parentProcess);
                        List<String> children = parentProcessMap.get(parentProcess);
                        if (children != null)
                        {
                            for (String child : children)
                            {
                                returnVal.add(child);
                            }
                        }
                        return returnVal;
                    }


                }
            }
            catch (IOException e)
            {
                LOGGER.error("Error whilst killing process " + _brokerCommand, e);
            }
        }
        return null;
    }

    private void doWindowsKill()
    {
        if(_windowsPids != null && !_windowsPids.isEmpty())
        {
            String parentProcess = _windowsPids.remove(0);
            try
            {

                Process p;
                for (String child : _windowsPids)
                {
                    p = Runtime.getRuntime().exec(new String[]{"taskkill", "/PID", child, "/T", "/F"});
                    consumeAllOutput(p);
                }
                p = Runtime.getRuntime().exec(new String[]{"taskkill", "/PID", parentProcess, "/T", "/F"});
                consumeAllOutput(p);

            }
            catch (IOException e)
            {
                LOGGER.error("Error whilst killing process " + _brokerCommand, e);
            }
        }
    }

    private static void consumeAllOutput(Process p) throws IOException
    {
        try(InputStreamReader inputStreamReader = new InputStreamReader(p.getInputStream()))
        {
            try (BufferedReader reader = new BufferedReader(inputStreamReader))
            {
                while (reader.readLine() != null)
                {
                }
            }
        }
    }

    @Override
    public void kill()
    {
        if (_pid == null)
        {
            if(SystemUtils.isWindows())
            {
                doWindowsKill();
            }
            LOGGER.info("Destroying broker process (no PID)");
            _process.destroy();
        }
        else
        {
            LOGGER.info("Killing broker process with PID " + _pid);
            sendSigkillForImmediateShutdown(_pid);
        }

        reapChildProcess();

        waitUntilPortsAreFreeIfRequired();
    }

    private void sendSigkillForImmediateShutdown(Integer pid)
    {
        boolean killSuccessful = false;
        try
        {
            final Process killProcess = Runtime.getRuntime().exec("kill -KILL " + pid);
            killProcess.waitFor();
            killSuccessful = killProcess.exitValue() == 0;
        }
        catch (IOException e)
        {
            LOGGER.error("Error whilst killing process " + _pid, e);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
        finally
        {
            if (!killSuccessful)
            {
                _process.destroy();
            }
        }
    }

    private Integer retrieveUnixPidIfPossible()
    {
        if(!SystemUtils.isWindows())
        {
            try
            {
                Integer pid = ReflectionUtils.getDeclaredField(_process, "pid");
                LOGGER.info("PID " + pid);
                return pid;
            }
            catch (ReflectionUtilsException e)
            {
                LOGGER.warn("Could not get pid for process, Broker process shutdown will be graceful");
            }
        }
        return null;
    }

    private void reapChildProcess()
    {
        try
        {
            _process.waitFor();
            LOGGER.info("broker exited: " + _process.exitValue());
        }
        catch (InterruptedException e)
        {
            LOGGER.error("Interrupted whilst waiting for process shutdown");
            Thread.currentThread().interrupt();
        }
        finally
        {
            try
            {
                _process.getInputStream().close();
                _process.getErrorStream().close();
                _process.getOutputStream().close();
            }
            catch (IOException e)
            {
            }
        }
    }

    @Override
    public String dumpThreads()
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try
        {
            Process process = Runtime.getRuntime().exec("jstack " + _pid);
            InputStream is = process.getInputStream();
            byte[] buffer = new byte[1024];
            int length = -1;
            while ((length = is.read(buffer)) != -1)
            {
                baos.write(buffer, 0, length);
            }
         }
        catch (Exception e)
        {
            LOGGER.error("Error whilst collecting thread dump for " + _pid, e);
        }
        return new String(baos.toByteArray());
    }

    @Override
    public String toString()
    {
        return "SpawnedBrokerHolder [_pid=" + _pid + ", _amqpPort="
                + getAmqpPort() + "]";
    }

    @Override
    protected TestBrokerConfiguration createBrokerConfiguration()
    {
        TestBrokerConfiguration configuration = super.createBrokerConfiguration();

        String remotelogback = "remotelogback";

        Map<String, String> mdc = new HashMap<>();
        mdc.put(QpidBrokerTestCase.CLASS_QUALIFIED_TEST_NAME, getClassQualifiedTestName());
        mdc.put("origin", getLogPrefix());

        Map<String, Object> loggerAttrs = new HashMap<>();
        loggerAttrs.put(BrokerLogger.TYPE, BrokerLogbackSocketLogger.TYPE);
        loggerAttrs.put(BrokerLogbackSocketLogger.NAME, remotelogback);
        loggerAttrs.put(BrokerLogbackSocketLogger.PORT, QpidBrokerTestCase.LOGBACK_REMOTE_PORT);
        loggerAttrs.put(BrokerLogbackSocketLogger.MAPPED_DIAGNOSTIC_CONTEXT, mdc);

        configuration.addObjectConfiguration(BrokerLogger.class, loggerAttrs);

        Map<String, Object> qpidRuleAttrs = new HashMap<>();
        qpidRuleAttrs.put(BrokerLogInclusionRule.NAME, "Qpid");
        qpidRuleAttrs.put(BrokerLogInclusionRule.TYPE, BrokerNameAndLevelLogInclusionRule.TYPE);
        qpidRuleAttrs.put(BrokerNameAndLevelLogInclusionRule.LEVEL, "DEBUG");
        qpidRuleAttrs.put(BrokerNameAndLevelLogInclusionRule.LOGGER_NAME, "org.apache.qpid.*");

        configuration.addObjectConfiguration(BrokerLogger.class, remotelogback,
                                             BrokerLogInclusionRule.class, qpidRuleAttrs);

        Map<String, Object> operationalLoggingRuleAttrs = new HashMap<>();
        operationalLoggingRuleAttrs.put(BrokerLogInclusionRule.NAME, "Operational");
        operationalLoggingRuleAttrs.put(BrokerLogInclusionRule.TYPE, BrokerNameAndLevelLogInclusionRule.TYPE);
        operationalLoggingRuleAttrs.put(BrokerNameAndLevelLogInclusionRule.LEVEL, "INFO");
        operationalLoggingRuleAttrs.put(BrokerNameAndLevelLogInclusionRule.LOGGER_NAME, "qpid.message.*");

        configuration.addObjectConfiguration(BrokerLogger.class, remotelogback,
                                             BrokerLogInclusionRule.class, operationalLoggingRuleAttrs);

        return configuration;
    }

}
