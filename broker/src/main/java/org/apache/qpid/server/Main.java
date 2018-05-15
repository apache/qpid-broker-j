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
package org.apache.qpid.server;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.CommonProperties;
import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.logging.logback.LogbackLoggingSystemLauncherListener;
import org.apache.qpid.server.model.AbstractSystemConfig;
import org.apache.qpid.server.model.JsonSystemConfigImpl;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.plugin.ProtocolEngineCreator;
import org.apache.qpid.server.plugin.QpidServiceLoader;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.server.util.StringUtil;

/**
 * Main entry point for Apache Qpid Broker-J.
 *
 */
public class Main
{
    public static final String PROPERTY_QPID_HOME = "QPID_HOME";
    /**
     * Configuration property name for the absolute path to use for the broker home directory.
     *
     * If not otherwise set, the value for this configuration property defaults to the location
     * set in the "QPID_HOME" system property if that was set, or remains unset if it was not.
     */
    private static final String QPID_HOME_DIR  = "qpid.home_dir";

    private static final int MANAGEMENT_MODE_PASSWORD_LENGTH = 10;

    private static final Option OPTION_HELP = Option.builder("h")
                                                    .desc("print this message")
                                                    .longOpt("help")
                                                    .build();

    private static final Option OPTION_VERSION = Option.builder("v")
                                                       .desc("print the version information and exit")
                                                       .longOpt("version")
                                                       .build();

    private static final Option OPTION_CONFIGURATION_STORE_PATH = Option.builder("sp")
                                                                        .argName("path")
                                                                        .hasArg()
                                                                        .desc("use given configuration store location")
                                                                        .longOpt("store-path")
                                                                        .build();

    private static final Option OPTION_CONFIGURATION_STORE_TYPE = Option.builder("st")
                                                                        .argName("type")
                                                                        .hasArg()
                                                                        .desc("use given broker configuration store "
                                                                              + "type")
                                                                        .longOpt("store-type")
                                                                        .build();

    private static final Option OPTION_INITIAL_CONFIGURATION_PATH = Option.builder("icp")
                                                                          .argName("path")
                                                                          .hasArg()
                                                                          .desc("set the location of initial JSON "
                                                                                + "config to use when "
                                                                                + "creating/overwriting a broker "
                                                                                + "configuration store")
                                                                          .longOpt("initial-config-path")
                                                                          .build();

    private static final Option OPTION_CREATE_INITIAL_CONFIG = Option.builder("cic")
                                                                     .argName("path")
                                                                     .numberOfArgs(1)
                                                                     .optionalArg(true)
                                                                     .desc("create a copy of the initial config file,"
                                                                           + " either to an"
                                                                           +
                                                                           " optionally specified file path, or as "
                                                                           + SystemConfig.DEFAULT_INITIAL_CONFIG_NAME
                                                                           + " in the current directory")
                                                                     .longOpt("create-initial-config")
                                                                     .build();

    private static final Option OPTION_CONFIGURATION_PROPERTY = Option.builder("prop")
                                                                      .argName("name=value").hasArg()
                                                                      .desc("set a configuration property to use when"
                                                                            + " resolving variables in the broker "
                                                                            + "configuration store, with format "
                                                                            + "\"name=value\"")
                                                                      .longOpt("config-property")
                                                                      .build();

    private static final Option OPTION_MANAGEMENT_MODE = Option.builder("mm")
                                                               .desc("start broker in management mode, disabling the "
                                                                     + "AMQP ports")
                                                               .longOpt("management-mode")
                                                               .build();

    private static final Option OPTION_MM_QUIESCE_VHOST_NODE = Option.builder("mmqv")
                                                                     .desc("make virtual host nodes stay in the "
                                                                           + "quiesced state during management mode")
                                                                     .longOpt("management-mode-quiesce-virtualhostnodes")
                                                                     .build();

    private static final Option OPTION_MM_HTTP_PORT = Option.builder("mmhttp")
                                                            .argName("port")
                                                            .hasArg()
                                                            .desc("override http management port in management mode")
                                                            .longOpt("management-mode-http-port")
                                                            .build();

    private static final Option OPTION_MM_PASSWORD = Option.builder("mmpass")
                                                           .argName("password")
                                                           .hasArg()
                                                           .desc("set the password for the management mode user "
                                                                 + SystemConfig.MANAGEMENT_MODE_USER_NAME)
                                                           .longOpt("management-mode-password")
                                                           .build();

    private static final Option OPTION_INITIAL_SYSTEM_PROPERTIES = Option.builder("props")
                                                                         .argName("path")
                                                                         .hasArg()
                                                                         .desc("set the location of initial properties file to set otherwise unset system properties")
                                                                         .longOpt("system-properties-file")
                                                                         .build();

    private static final Options OPTIONS = new Options();

    static
    {
        OPTIONS.addOption(OPTION_HELP);
        OPTIONS.addOption(OPTION_VERSION);
        OPTIONS.addOption(OPTION_CONFIGURATION_STORE_PATH);
        OPTIONS.addOption(OPTION_CONFIGURATION_STORE_TYPE);
        OPTIONS.addOption(OPTION_CREATE_INITIAL_CONFIG);
        OPTIONS.addOption(OPTION_INITIAL_CONFIGURATION_PATH);
        OPTIONS.addOption(OPTION_MANAGEMENT_MODE);
        OPTIONS.addOption(OPTION_MM_QUIESCE_VHOST_NODE);
        OPTIONS.addOption(OPTION_MM_HTTP_PORT);
        OPTIONS.addOption(OPTION_MM_PASSWORD);
        OPTIONS.addOption(OPTION_CONFIGURATION_PROPERTY);
        OPTIONS.addOption(OPTION_INITIAL_SYSTEM_PROPERTIES);

        CommonProperties.ensureIsLoaded();
    }

    protected CommandLine _commandLine;

    public static void main(String[] args)
    {
        new Main(args);
    }

    public Main(final String[] args)
    {
        if (parseCommandline(args))
        {
            try
            {
                execute();
            }
            catch(Exception e)
            {
                System.err.println("Exception during startup: " + e);
                e.printStackTrace();
                shutdown(1);
            }
        }
    }

    protected boolean parseCommandline(final String[] args)
    {
        try
        {
            _commandLine = new DefaultParser().parse(OPTIONS, args);

            return true;
        }
        catch (ParseException e)
        {
            System.err.println("Error: " + e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Qpid", OPTIONS, true);

            return false;
        }
    }

    protected void execute() throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();

        String initialProperties = _commandLine.getOptionValue(OPTION_INITIAL_SYSTEM_PROPERTIES.getOpt());
        SystemLauncher.populateSystemPropertiesFromDefaults(initialProperties);

        String initialConfigLocation = _commandLine.getOptionValue(OPTION_INITIAL_CONFIGURATION_PATH.getOpt());
        if(initialConfigLocation != null)
        {
            attributes.put(SystemConfig.INITIAL_CONFIGURATION_LOCATION, initialConfigLocation);
        }

        //process the remaining options
        if (_commandLine.hasOption(OPTION_HELP.getOpt()))
        {
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Qpid", OPTIONS, true);
        }
        else if (_commandLine.hasOption(OPTION_CREATE_INITIAL_CONFIG.getOpt()))
        {
            createInitialConfigCopy(initialConfigLocation);
        }
        else if (_commandLine.hasOption(OPTION_VERSION.getOpt()))
        {
            printVersion();
        }
        else
        {
            String[] configPropPairs = _commandLine.getOptionValues(OPTION_CONFIGURATION_PROPERTY.getOpt());

            Map<String, String> context = calculateConfigContext(configPropPairs);
            if(!context.isEmpty())
            {
                attributes.put(SystemConfig.CONTEXT, context);
            }

            String configurationStore = _commandLine.getOptionValue(OPTION_CONFIGURATION_STORE_PATH.getOpt());
            if(configurationStore != null)
            {
                attributes.put("storePath", configurationStore);
            }
            String configurationStoreType = _commandLine.getOptionValue(OPTION_CONFIGURATION_STORE_TYPE.getOpt());
            attributes.put(SystemConfig.TYPE, configurationStoreType == null ? JsonSystemConfigImpl.SYSTEM_CONFIG_TYPE : configurationStoreType);

            boolean managementMode = _commandLine.hasOption(OPTION_MANAGEMENT_MODE.getOpt());
            if (managementMode)
            {
                attributes.put(SystemConfig.MANAGEMENT_MODE, true);
                String httpPort = _commandLine.getOptionValue(OPTION_MM_HTTP_PORT.getOpt());
                if(httpPort != null)
                {
                    attributes.put(SystemConfig.MANAGEMENT_MODE_HTTP_PORT_OVERRIDE, httpPort);
                }

                boolean quiesceVhosts = _commandLine.hasOption(OPTION_MM_QUIESCE_VHOST_NODE.getOpt());
                attributes.put(SystemConfig.MANAGEMENT_MODE_QUIESCE_VIRTUAL_HOSTS, quiesceVhosts);

                String password = _commandLine.getOptionValue(OPTION_MM_PASSWORD.getOpt());
                if (password == null)
                {
                    password = new StringUtil().randomAlphaNumericString(MANAGEMENT_MODE_PASSWORD_LENGTH);
                }
                attributes.put(SystemConfig.MANAGEMENT_MODE_PASSWORD, password);
            }
            setExceptionHandler();

            startBroker(attributes);
        }
    }

    private Map<String, String> calculateConfigContext(final String[] configPropPairs)
    {
        Map<String,String> context = new HashMap<>();

        if(configPropPairs != null && configPropPairs.length > 0)
        {
            for(String s : configPropPairs)
            {
                int firstEquals = s.indexOf("=");
                if(firstEquals == -1)
                {
                    throw new IllegalArgumentException("Configuration property argument is not of the format name=value: " + s);
                }
                String name = s.substring(0, firstEquals);
                String value = s.substring(firstEquals + 1);

                if(name.equals(""))
                {
                    throw new IllegalArgumentException("Configuration property argument is not of the format name=value: " + s);
                }

                context.put(name, value);
            }
        }
        if(!context.containsKey(QPID_HOME_DIR))
        {
            Properties systemProperties = System.getProperties();
            final Map<String, String> environment = System.getenv();
            if(systemProperties.containsKey(QPID_HOME_DIR))
            {
                context.put(QPID_HOME_DIR, systemProperties.getProperty(QPID_HOME_DIR));
            }
            else if(environment.containsKey(QPID_HOME_DIR))
            {
                context.put(QPID_HOME_DIR, environment.get(QPID_HOME_DIR));
            }
            else if(context.containsKey(PROPERTY_QPID_HOME))
            {
                context.put(QPID_HOME_DIR, context.get(PROPERTY_QPID_HOME));
            }
            else if(systemProperties.containsKey(PROPERTY_QPID_HOME))
            {
                context.put(QPID_HOME_DIR, systemProperties.getProperty(PROPERTY_QPID_HOME));
            }
            else if(environment.containsKey(PROPERTY_QPID_HOME))
            {
                context.put(QPID_HOME_DIR, environment.get(PROPERTY_QPID_HOME));
            }
        }
        return context;
    }

    private void printVersion()
    {
        final StringBuilder protocol = new StringBuilder("AMQP version(s) [major.minor]: ");
        boolean first = true;
        Set<Protocol> protocols = new TreeSet<>();
        for(ProtocolEngineCreator installedEngine : (new QpidServiceLoader()).instancesOf(ProtocolEngineCreator.class))
        {
            protocols.add(installedEngine.getVersion());
        }
        for(Protocol supportedProtocol : protocols)
        {
            if (first)
            {
                first = false;
            }
            else
            {
                protocol.append(", ");
            }

            protocol.append(supportedProtocol.getProtocolVersion());
        }

        System.out.println(CommonProperties.getVersionString() + " (" + protocol + ")");
    }

    private void createInitialConfigCopy(String initialConfigLocation)
    {
        File destinationFile = null;

        String destinationOption = _commandLine.getOptionValue(OPTION_CREATE_INITIAL_CONFIG.getOpt());
        if (destinationOption != null)
        {
            destinationFile = new File(destinationOption);
        }
        else
        {
            destinationFile = new File(System.getProperty("user.dir"), SystemConfig.DEFAULT_INITIAL_CONFIG_NAME);
        }

        if(initialConfigLocation == null)
        {
            initialConfigLocation = AbstractSystemConfig.getDefaultValue(SystemConfig.INITIAL_CONFIGURATION_LOCATION);
        }
        copyInitialConfigFile(initialConfigLocation, destinationFile);

        System.out.println("Initial config written to: " + destinationFile.getAbsolutePath());
    }

    private void copyInitialConfigFile(final String initialConfigLocation, final File destinationFile)
    {
        URL url = null;
        try
        {
            url = new URL(initialConfigLocation);
        }
        catch (MalformedURLException e)
        {
            File locationFile = new File(initialConfigLocation);
            try
            {
                url = locationFile.toURI().toURL();
            }
            catch (MalformedURLException e1)
            {
                throw new IllegalConfigurationException("Cannot create URL for file " + locationFile, e1);
            }
        }
        InputStream in =  null;
        try
        {
            in = url.openStream();
            FileUtils.copy(in, destinationFile);
        }
        catch (IOException e)
        {
            throw new IllegalConfigurationException("Cannot create file " + destinationFile
                                                    + " by copying initial config from " + initialConfigLocation, e);
        }
        finally
        {
            if (in != null)
            {
                try
                {
                    in.close();
                }
                catch (IOException e)
                {
                    throw new IllegalConfigurationException("Cannot close initial config input stream: " + initialConfigLocation, e);
                }
            }
        }
    }

    protected void setExceptionHandler()
    {
        Thread.UncaughtExceptionHandler handler = null;
        String handlerClass = System.getProperty("qpid.broker.exceptionHandler");
        if(handlerClass != null)
        {
            try
            {
                handler = (Thread.UncaughtExceptionHandler) Class.forName(handlerClass).newInstance();
            }
            catch (ClassNotFoundException | InstantiationException | IllegalAccessException | ClassCastException e)
            {
                
            }
        }
        
        if(handler == null)
        {
            handler =
                new Thread.UncaughtExceptionHandler()
                {
                    @Override
                    public void uncaughtException(final Thread t, final Throwable e)
                    {
                        boolean continueOnError = Boolean.getBoolean("qpid.broker.exceptionHandler.continue");
                        try
                        {
                            System.err.println("########################################################################");
                            System.err.println("#");
                            System.err.print("# Unhandled Exception ");
                            System.err.print(e.toString());
                            System.err.print(" in Thread ");
                            System.err.println(t.getName());
                            System.err.println("#");
                            System.err.println(continueOnError ? "# Forced to continue by JVM setting 'qpid.broker.exceptionHandler.continue'" : "# Exiting");
                            System.err.println("#");
                            System.err.println("########################################################################");
                            e.printStackTrace(System.err);

                            Logger logger = LoggerFactory.getLogger("org.apache.qpid.server.Main");
                            logger.error("Uncaught exception, " + (continueOnError ? "continuing." : "shutting down."), e);
                        }
                        finally
                        {
                            if (!continueOnError)
                            {
                                Runtime.getRuntime().halt(1);
                            }
                        }

                    }
                };

            Thread.setDefaultUncaughtExceptionHandler(handler);
        } 
    }

    protected void startBroker(Map<String,Object> attributes) throws Exception
    {
        SystemLauncher systemLauncher = new SystemLauncher(new LogbackLoggingSystemLauncherListener(),
                                                           new SystemLauncherListener.DefaultSystemLauncherListener()
                                                           {
                                                               @Override
                                                               public void onShutdown(final int exitCode)
                                                               {
                                                                   if (exitCode != 0)
                                                                   {
                                                                       shutdown(exitCode);
                                                                   }
                                                               }
                                                           });

        systemLauncher.startup(attributes);
    }

    protected void shutdown(final int status)
    {
        System.exit(status);
    }

}
