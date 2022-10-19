/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 *
 */
package org.apache.qpid.server.logging.messages;

import static org.apache.qpid.server.logging.AbstractMessageLogger.DEFAULT_LOG_HIERARCHY_PREFIX;

import java.text.MessageFormat;
import java.util.Locale;
import java.util.ResourceBundle;

import org.slf4j.LoggerFactory;

import org.apache.qpid.server.logging.LogMessage;

/**
 * DO NOT EDIT DIRECTLY, THIS FILE WAS GENERATED.
 *
 * Generated using GenerateLogMessages and LogMessages.vm
 * This file is based on the content of HighAvailability_logmessages.properties
 *
 * To regenerate, use Maven lifecycle generates-sources with -Dgenerate=true
 */
public class HighAvailabilityMessages
{
    private static final ResourceBundle MESSAGES;
    private static final Locale CURRENT_LOCALE;

    static
    {
        Locale locale = Locale.US;
        String localeSetting = System.getProperty("qpid.broker_locale");
        if (localeSetting != null)
        {
            String[] localeParts = localeSetting.split("_");
            String language = (localeParts.length > 0 ? localeParts[0] : "");
            String country = (localeParts.length > 1 ? localeParts[1] : "");
            String variant = "";
            if (localeParts.length > 2)
            {
                variant = localeSetting.substring(language.length() + 1 + country.length() + 1);
            }
            locale = new Locale(language, country, variant);
        }
        CURRENT_LOCALE = locale;
    }

    public static final String HIGHAVAILABILITY_LOG_HIERARCHY = DEFAULT_LOG_HIERARCHY_PREFIX + "highavailability";
    public static final String ADDED_LOG_HIERARCHY = DEFAULT_LOG_HIERARCHY_PREFIX + "highavailability.added";
    public static final String CREATE_LOG_HIERARCHY = DEFAULT_LOG_HIERARCHY_PREFIX + "highavailability.create";
    public static final String DELETE_LOG_HIERARCHY = DEFAULT_LOG_HIERARCHY_PREFIX + "highavailability.delete";
    public static final String DESIGNATED_PRIMARY_CHANGED_LOG_HIERARCHY = DEFAULT_LOG_HIERARCHY_PREFIX + "highavailability.designated_primary_changed";
    public static final String INTRUDER_DETECTED_LOG_HIERARCHY = DEFAULT_LOG_HIERARCHY_PREFIX + "highavailability.intruder_detected";
    public static final String JOINED_LOG_HIERARCHY = DEFAULT_LOG_HIERARCHY_PREFIX + "highavailability.joined";
    public static final String LEFT_LOG_HIERARCHY = DEFAULT_LOG_HIERARCHY_PREFIX + "highavailability.left";
    public static final String NODE_ROLLEDBACK_LOG_HIERARCHY = DEFAULT_LOG_HIERARCHY_PREFIX + "highavailability.node_rolledback";
    public static final String PRIORITY_CHANGED_LOG_HIERARCHY = DEFAULT_LOG_HIERARCHY_PREFIX + "highavailability.priority_changed";
    public static final String QUORUM_LOST_LOG_HIERARCHY = DEFAULT_LOG_HIERARCHY_PREFIX + "highavailability.quorum_lost";
    public static final String QUORUM_OVERRIDE_CHANGED_LOG_HIERARCHY = DEFAULT_LOG_HIERARCHY_PREFIX + "highavailability.quorum_override_changed";
    public static final String REMOVED_LOG_HIERARCHY = DEFAULT_LOG_HIERARCHY_PREFIX + "highavailability.removed";
    public static final String ROLE_CHANGED_LOG_HIERARCHY = DEFAULT_LOG_HIERARCHY_PREFIX + "highavailability.role_changed";
    public static final String TRANSFER_MASTER_LOG_HIERARCHY = DEFAULT_LOG_HIERARCHY_PREFIX + "highavailability.transfer_master";
    public static final String UPDATE_LOG_HIERARCHY = DEFAULT_LOG_HIERARCHY_PREFIX + "highavailability.update";

    static
    {
        LoggerFactory.getLogger(HIGHAVAILABILITY_LOG_HIERARCHY);
        LoggerFactory.getLogger(ADDED_LOG_HIERARCHY);
        LoggerFactory.getLogger(CREATE_LOG_HIERARCHY);
        LoggerFactory.getLogger(DELETE_LOG_HIERARCHY);
        LoggerFactory.getLogger(DESIGNATED_PRIMARY_CHANGED_LOG_HIERARCHY);
        LoggerFactory.getLogger(INTRUDER_DETECTED_LOG_HIERARCHY);
        LoggerFactory.getLogger(JOINED_LOG_HIERARCHY);
        LoggerFactory.getLogger(LEFT_LOG_HIERARCHY);
        LoggerFactory.getLogger(NODE_ROLLEDBACK_LOG_HIERARCHY);
        LoggerFactory.getLogger(PRIORITY_CHANGED_LOG_HIERARCHY);
        LoggerFactory.getLogger(QUORUM_LOST_LOG_HIERARCHY);
        LoggerFactory.getLogger(QUORUM_OVERRIDE_CHANGED_LOG_HIERARCHY);
        LoggerFactory.getLogger(REMOVED_LOG_HIERARCHY);
        LoggerFactory.getLogger(ROLE_CHANGED_LOG_HIERARCHY);
        LoggerFactory.getLogger(TRANSFER_MASTER_LOG_HIERARCHY);
        LoggerFactory.getLogger(UPDATE_LOG_HIERARCHY);

        MESSAGES = ResourceBundle.getBundle("org.apache.qpid.server.logging.messages.HighAvailability_logmessages", CURRENT_LOCALE);
    }

    /**
     * Log a HighAvailability message of the Format:
     * <pre>HA-1003 : Added : Node : ''{0}'' ({1})</pre>
     * Optional values are contained in [square brackets] and are numbered
     * sequentially in the method call.
     *
     */
    public static LogMessage ADDED(String param1, String param2)
    {
        String rawMessage = MESSAGES.getString("ADDED");

        final Object[] messageArguments = {param1, param2};
        // Create a new MessageFormat to ensure thread safety.
        // Sharing a MessageFormat and using applyPattern is not thread safe
        MessageFormat formatter = new MessageFormat(rawMessage, CURRENT_LOCALE);

        final String message = formatter.format(messageArguments);

        return new LogMessage()
        {
            @Override
            public String toString()
            {
                return message;
            }

            @Override
            public String getLogHierarchy()
            {
                return ADDED_LOG_HIERARCHY;
            }

            @Override
            public boolean equals(final Object o)
            {
                if (this == o)
                {
                    return true;
                }
                if (o == null || getClass() != o.getClass())
                {
                    return false;
                }

                final LogMessage that = (LogMessage) o;

                return getLogHierarchy().equals(that.getLogHierarchy()) && toString().equals(that.toString());

            }

            @Override
            public int hashCode()
            {
                int result = toString().hashCode();
                result = 31 * result + getLogHierarchy().hashCode();
                return result;
            }
        };
    }

    /**
     * Log a HighAvailability message of the Format:
     * <pre>HA-1001 : Create : "{0}" : {1} : {2}</pre>
     * Optional values are contained in [square brackets] and are numbered
     * sequentially in the method call.
     *
     */
    public static LogMessage CREATE(String param1, String param2, String param3)
    {
        String rawMessage = MESSAGES.getString("CREATE");

        final Object[] messageArguments = {param1, param2, param3};
        // Create a new MessageFormat to ensure thread safety.
        // Sharing a MessageFormat and using applyPattern is not thread safe
        MessageFormat formatter = new MessageFormat(rawMessage, CURRENT_LOCALE);

        final String message = formatter.format(messageArguments);

        return new LogMessage()
        {
            @Override
            public String toString()
            {
                return message;
            }

            @Override
            public String getLogHierarchy()
            {
                return CREATE_LOG_HIERARCHY;
            }

            @Override
            public boolean equals(final Object o)
            {
                if (this == o)
                {
                    return true;
                }
                if (o == null || getClass() != o.getClass())
                {
                    return false;
                }

                final LogMessage that = (LogMessage) o;

                return getLogHierarchy().equals(that.getLogHierarchy()) && toString().equals(that.toString());

            }

            @Override
            public int hashCode()
            {
                int result = toString().hashCode();
                result = 31 * result + getLogHierarchy().hashCode();
                return result;
            }
        };
    }

    /**
     * Log a HighAvailability message of the Format:
     * <pre>HA-1002 : Delete : "{0}" : {1}</pre>
     * Optional values are contained in [square brackets] and are numbered
     * sequentially in the method call.
     *
     */
    public static LogMessage DELETE(String param1, String param2)
    {
        String rawMessage = MESSAGES.getString("DELETE");

        final Object[] messageArguments = {param1, param2};
        // Create a new MessageFormat to ensure thread safety.
        // Sharing a MessageFormat and using applyPattern is not thread safe
        MessageFormat formatter = new MessageFormat(rawMessage, CURRENT_LOCALE);

        final String message = formatter.format(messageArguments);

        return new LogMessage()
        {
            @Override
            public String toString()
            {
                return message;
            }

            @Override
            public String getLogHierarchy()
            {
                return DELETE_LOG_HIERARCHY;
            }

            @Override
            public boolean equals(final Object o)
            {
                if (this == o)
                {
                    return true;
                }
                if (o == null || getClass() != o.getClass())
                {
                    return false;
                }

                final LogMessage that = (LogMessage) o;

                return getLogHierarchy().equals(that.getLogHierarchy()) && toString().equals(that.toString());

            }

            @Override
            public int hashCode()
            {
                int result = toString().hashCode();
                result = 31 * result + getLogHierarchy().hashCode();
                return result;
            }
        };
    }

    /**
     * Log a HighAvailability message of the Format:
     * <pre>HA-1013 : Designated primary : {0}</pre>
     * Optional values are contained in [square brackets] and are numbered
     * sequentially in the method call.
     *
     */
    public static LogMessage DESIGNATED_PRIMARY_CHANGED(String param1)
    {
        String rawMessage = MESSAGES.getString("DESIGNATED_PRIMARY_CHANGED");

        final Object[] messageArguments = {param1};
        // Create a new MessageFormat to ensure thread safety.
        // Sharing a MessageFormat and using applyPattern is not thread safe
        MessageFormat formatter = new MessageFormat(rawMessage, CURRENT_LOCALE);

        final String message = formatter.format(messageArguments);

        return new LogMessage()
        {
            @Override
            public String toString()
            {
                return message;
            }

            @Override
            public String getLogHierarchy()
            {
                return DESIGNATED_PRIMARY_CHANGED_LOG_HIERARCHY;
            }

            @Override
            public boolean equals(final Object o)
            {
                if (this == o)
                {
                    return true;
                }
                if (o == null || getClass() != o.getClass())
                {
                    return false;
                }

                final LogMessage that = (LogMessage) o;

                return getLogHierarchy().equals(that.getLogHierarchy()) && toString().equals(that.toString());

            }

            @Override
            public int hashCode()
            {
                int result = toString().hashCode();
                result = 31 * result + getLogHierarchy().hashCode();
                return result;
            }
        };
    }

    /**
     * Log a HighAvailability message of the Format:
     * <pre>HA-1008 : Intruder detected : Node ''{0}'' ({1})</pre>
     * Optional values are contained in [square brackets] and are numbered
     * sequentially in the method call.
     *
     */
    public static LogMessage INTRUDER_DETECTED(String param1, String param2)
    {
        String rawMessage = MESSAGES.getString("INTRUDER_DETECTED");

        final Object[] messageArguments = {param1, param2};
        // Create a new MessageFormat to ensure thread safety.
        // Sharing a MessageFormat and using applyPattern is not thread safe
        MessageFormat formatter = new MessageFormat(rawMessage, CURRENT_LOCALE);

        final String message = formatter.format(messageArguments);

        return new LogMessage()
        {
            @Override
            public String toString()
            {
                return message;
            }

            @Override
            public String getLogHierarchy()
            {
                return INTRUDER_DETECTED_LOG_HIERARCHY;
            }

            @Override
            public boolean equals(final Object o)
            {
                if (this == o)
                {
                    return true;
                }
                if (o == null || getClass() != o.getClass())
                {
                    return false;
                }

                final LogMessage that = (LogMessage) o;

                return getLogHierarchy().equals(that.getLogHierarchy()) && toString().equals(that.toString());

            }

            @Override
            public int hashCode()
            {
                int result = toString().hashCode();
                result = 31 * result + getLogHierarchy().hashCode();
                return result;
            }
        };
    }

    /**
     * Log a HighAvailability message of the Format:
     * <pre>HA-1005 : Joined : Node : ''{0}'' ({1})</pre>
     * Optional values are contained in [square brackets] and are numbered
     * sequentially in the method call.
     *
     */
    public static LogMessage JOINED(String param1, String param2)
    {
        String rawMessage = MESSAGES.getString("JOINED");

        final Object[] messageArguments = {param1, param2};
        // Create a new MessageFormat to ensure thread safety.
        // Sharing a MessageFormat and using applyPattern is not thread safe
        MessageFormat formatter = new MessageFormat(rawMessage, CURRENT_LOCALE);

        final String message = formatter.format(messageArguments);

        return new LogMessage()
        {
            @Override
            public String toString()
            {
                return message;
            }

            @Override
            public String getLogHierarchy()
            {
                return JOINED_LOG_HIERARCHY;
            }

            @Override
            public boolean equals(final Object o)
            {
                if (this == o)
                {
                    return true;
                }
                if (o == null || getClass() != o.getClass())
                {
                    return false;
                }

                final LogMessage that = (LogMessage) o;

                return getLogHierarchy().equals(that.getLogHierarchy()) && toString().equals(that.toString());

            }

            @Override
            public int hashCode()
            {
                int result = toString().hashCode();
                result = 31 * result + getLogHierarchy().hashCode();
                return result;
            }
        };
    }

    /**
     * Log a HighAvailability message of the Format:
     * <pre>HA-1006 : Left : Node : ''{0}'' ({1})</pre>
     * Optional values are contained in [square brackets] and are numbered
     * sequentially in the method call.
     *
     */
    public static LogMessage LEFT(String param1, String param2)
    {
        String rawMessage = MESSAGES.getString("LEFT");

        final Object[] messageArguments = {param1, param2};
        // Create a new MessageFormat to ensure thread safety.
        // Sharing a MessageFormat and using applyPattern is not thread safe
        MessageFormat formatter = new MessageFormat(rawMessage, CURRENT_LOCALE);

        final String message = formatter.format(messageArguments);

        return new LogMessage()
        {
            @Override
            public String toString()
            {
                return message;
            }

            @Override
            public String getLogHierarchy()
            {
                return LEFT_LOG_HIERARCHY;
            }

            @Override
            public boolean equals(final Object o)
            {
                if (this == o)
                {
                    return true;
                }
                if (o == null || getClass() != o.getClass())
                {
                    return false;
                }

                final LogMessage that = (LogMessage) o;

                return getLogHierarchy().equals(that.getLogHierarchy()) && toString().equals(that.toString());

            }

            @Override
            public int hashCode()
            {
                int result = toString().hashCode();
                result = 31 * result + getLogHierarchy().hashCode();
                return result;
            }
        };
    }

    /**
     * Log a HighAvailability message of the Format:
     * <pre>HA-1014 : Diverged transactions discarded</pre>
     * Optional values are contained in [square brackets] and are numbered
     * sequentially in the method call.
     *
     */
    public static LogMessage NODE_ROLLEDBACK()
    {
        String rawMessage = MESSAGES.getString("NODE_ROLLEDBACK");

        final String message = rawMessage;

        return new LogMessage()
        {
            @Override
            public String toString()
            {
                return message;
            }

            @Override
            public String getLogHierarchy()
            {
                return NODE_ROLLEDBACK_LOG_HIERARCHY;
            }

            @Override
            public boolean equals(final Object o)
            {
                if (this == o)
                {
                    return true;
                }
                if (o == null || getClass() != o.getClass())
                {
                    return false;
                }

                final LogMessage that = (LogMessage) o;

                return getLogHierarchy().equals(that.getLogHierarchy()) && toString().equals(that.toString());

            }

            @Override
            public int hashCode()
            {
                int result = toString().hashCode();
                result = 31 * result + getLogHierarchy().hashCode();
                return result;
            }
        };
    }

    /**
     * Log a HighAvailability message of the Format:
     * <pre>HA-1012 : Priority : {0}</pre>
     * Optional values are contained in [square brackets] and are numbered
     * sequentially in the method call.
     *
     */
    public static LogMessage PRIORITY_CHANGED(String param1)
    {
        String rawMessage = MESSAGES.getString("PRIORITY_CHANGED");

        final Object[] messageArguments = {param1};
        // Create a new MessageFormat to ensure thread safety.
        // Sharing a MessageFormat and using applyPattern is not thread safe
        MessageFormat formatter = new MessageFormat(rawMessage, CURRENT_LOCALE);

        final String message = formatter.format(messageArguments);

        return new LogMessage()
        {
            @Override
            public String toString()
            {
                return message;
            }

            @Override
            public String getLogHierarchy()
            {
                return PRIORITY_CHANGED_LOG_HIERARCHY;
            }

            @Override
            public boolean equals(final Object o)
            {
                if (this == o)
                {
                    return true;
                }
                if (o == null || getClass() != o.getClass())
                {
                    return false;
                }

                final LogMessage that = (LogMessage) o;

                return getLogHierarchy().equals(that.getLogHierarchy()) && toString().equals(that.toString());

            }

            @Override
            public int hashCode()
            {
                int result = toString().hashCode();
                result = 31 * result + getLogHierarchy().hashCode();
                return result;
            }
        };
    }

    /**
     * Log a HighAvailability message of the Format:
     * <pre>HA-1009 : Insufficient replicas contactable</pre>
     * Optional values are contained in [square brackets] and are numbered
     * sequentially in the method call.
     *
     */
    public static LogMessage QUORUM_LOST()
    {
        String rawMessage = MESSAGES.getString("QUORUM_LOST");

        final String message = rawMessage;

        return new LogMessage()
        {
            @Override
            public String toString()
            {
                return message;
            }

            @Override
            public String getLogHierarchy()
            {
                return QUORUM_LOST_LOG_HIERARCHY;
            }

            @Override
            public boolean equals(final Object o)
            {
                if (this == o)
                {
                    return true;
                }
                if (o == null || getClass() != o.getClass())
                {
                    return false;
                }

                final LogMessage that = (LogMessage) o;

                return getLogHierarchy().equals(that.getLogHierarchy()) && toString().equals(that.toString());

            }

            @Override
            public int hashCode()
            {
                int result = toString().hashCode();
                result = 31 * result + getLogHierarchy().hashCode();
                return result;
            }
        };
    }

    /**
     * Log a HighAvailability message of the Format:
     * <pre>HA-1011 : Minimum group size : {0}</pre>
     * Optional values are contained in [square brackets] and are numbered
     * sequentially in the method call.
     *
     */
    public static LogMessage QUORUM_OVERRIDE_CHANGED(String param1)
    {
        String rawMessage = MESSAGES.getString("QUORUM_OVERRIDE_CHANGED");

        final Object[] messageArguments = {param1};
        // Create a new MessageFormat to ensure thread safety.
        // Sharing a MessageFormat and using applyPattern is not thread safe
        MessageFormat formatter = new MessageFormat(rawMessage, CURRENT_LOCALE);

        final String message = formatter.format(messageArguments);

        return new LogMessage()
        {
            @Override
            public String toString()
            {
                return message;
            }

            @Override
            public String getLogHierarchy()
            {
                return QUORUM_OVERRIDE_CHANGED_LOG_HIERARCHY;
            }

            @Override
            public boolean equals(final Object o)
            {
                if (this == o)
                {
                    return true;
                }
                if (o == null || getClass() != o.getClass())
                {
                    return false;
                }

                final LogMessage that = (LogMessage) o;

                return getLogHierarchy().equals(that.getLogHierarchy()) && toString().equals(that.toString());

            }

            @Override
            public int hashCode()
            {
                int result = toString().hashCode();
                result = 31 * result + getLogHierarchy().hashCode();
                return result;
            }
        };
    }

    /**
     * Log a HighAvailability message of the Format:
     * <pre>HA-1004 : Removed : Node : ''{0}'' ({1})</pre>
     * Optional values are contained in [square brackets] and are numbered
     * sequentially in the method call.
     *
     */
    public static LogMessage REMOVED(String param1, String param2)
    {
        String rawMessage = MESSAGES.getString("REMOVED");

        final Object[] messageArguments = {param1, param2};
        // Create a new MessageFormat to ensure thread safety.
        // Sharing a MessageFormat and using applyPattern is not thread safe
        MessageFormat formatter = new MessageFormat(rawMessage, CURRENT_LOCALE);

        final String message = formatter.format(messageArguments);

        return new LogMessage()
        {
            @Override
            public String toString()
            {
                return message;
            }

            @Override
            public String getLogHierarchy()
            {
                return REMOVED_LOG_HIERARCHY;
            }

            @Override
            public boolean equals(final Object o)
            {
                if (this == o)
                {
                    return true;
                }
                if (o == null || getClass() != o.getClass())
                {
                    return false;
                }

                final LogMessage that = (LogMessage) o;

                return getLogHierarchy().equals(that.getLogHierarchy()) && toString().equals(that.toString());

            }

            @Override
            public int hashCode()
            {
                int result = toString().hashCode();
                result = 31 * result + getLogHierarchy().hashCode();
                return result;
            }
        };
    }

    /**
     * Log a HighAvailability message of the Format:
     * <pre>HA-1010 : Role change reported: Node : ''{0}'' ({1}) : from ''{2}'' to ''{3}''</pre>
     * Optional values are contained in [square brackets] and are numbered
     * sequentially in the method call.
     *
     */
    public static LogMessage ROLE_CHANGED(String param1, String param2, String param3, String param4)
    {
        String rawMessage = MESSAGES.getString("ROLE_CHANGED");

        final Object[] messageArguments = {param1, param2, param3, param4};
        // Create a new MessageFormat to ensure thread safety.
        // Sharing a MessageFormat and using applyPattern is not thread safe
        MessageFormat formatter = new MessageFormat(rawMessage, CURRENT_LOCALE);

        final String message = formatter.format(messageArguments);

        return new LogMessage()
        {
            @Override
            public String toString()
            {
                return message;
            }

            @Override
            public String getLogHierarchy()
            {
                return ROLE_CHANGED_LOG_HIERARCHY;
            }

            @Override
            public boolean equals(final Object o)
            {
                if (this == o)
                {
                    return true;
                }
                if (o == null || getClass() != o.getClass())
                {
                    return false;
                }

                final LogMessage that = (LogMessage) o;

                return getLogHierarchy().equals(that.getLogHierarchy()) && toString().equals(that.toString());

            }

            @Override
            public int hashCode()
            {
                int result = toString().hashCode();
                result = 31 * result + getLogHierarchy().hashCode();
                return result;
            }
        };
    }

    /**
     * Log a HighAvailability message of the Format:
     * <pre>HA-1007 : Master transfer requested : to ''{0}'' ({1})</pre>
     * Optional values are contained in [square brackets] and are numbered
     * sequentially in the method call.
     *
     */
    public static LogMessage TRANSFER_MASTER(String param1, String param2)
    {
        String rawMessage = MESSAGES.getString("TRANSFER_MASTER");

        final Object[] messageArguments = {param1, param2};
        // Create a new MessageFormat to ensure thread safety.
        // Sharing a MessageFormat and using applyPattern is not thread safe
        MessageFormat formatter = new MessageFormat(rawMessage, CURRENT_LOCALE);

        final String message = formatter.format(messageArguments);

        return new LogMessage()
        {
            @Override
            public String toString()
            {
                return message;
            }

            @Override
            public String getLogHierarchy()
            {
                return TRANSFER_MASTER_LOG_HIERARCHY;
            }

            @Override
            public boolean equals(final Object o)
            {
                if (this == o)
                {
                    return true;
                }
                if (o == null || getClass() != o.getClass())
                {
                    return false;
                }

                final LogMessage that = (LogMessage) o;

                return getLogHierarchy().equals(that.getLogHierarchy()) && toString().equals(that.toString());

            }

            @Override
            public int hashCode()
            {
                int result = toString().hashCode();
                result = 31 * result + getLogHierarchy().hashCode();
                return result;
            }
        };
    }

    /**
     * Log a HighAvailability message of the Format:
     * <pre>HA-1015 : Update : "{0}" : {1} : {2}</pre>
     * Optional values are contained in [square brackets] and are numbered
     * sequentially in the method call.
     *
     */
    public static LogMessage UPDATE(String param1, String param2, String param3)
    {
        String rawMessage = MESSAGES.getString("UPDATE");

        final Object[] messageArguments = {param1, param2, param3};
        // Create a new MessageFormat to ensure thread safety.
        // Sharing a MessageFormat and using applyPattern is not thread safe
        MessageFormat formatter = new MessageFormat(rawMessage, CURRENT_LOCALE);

        final String message = formatter.format(messageArguments);

        return new LogMessage()
        {
            @Override
            public String toString()
            {
                return message;
            }

            @Override
            public String getLogHierarchy()
            {
                return UPDATE_LOG_HIERARCHY;
            }

            @Override
            public boolean equals(final Object o)
            {
                if (this == o)
                {
                    return true;
                }
                if (o == null || getClass() != o.getClass())
                {
                    return false;
                }

                final LogMessage that = (LogMessage) o;

                return getLogHierarchy().equals(that.getLogHierarchy()) && toString().equals(that.toString());

            }

            @Override
            public int hashCode()
            {
                int result = toString().hashCode();
                result = 31 * result + getLogHierarchy().hashCode();
                return result;
            }
        };
    }


    private HighAvailabilityMessages()
    {
    }

}
