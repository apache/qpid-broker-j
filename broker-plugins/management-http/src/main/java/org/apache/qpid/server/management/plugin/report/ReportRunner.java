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
package org.apache.qpid.server.management.plugin.report;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.MessageInfoImpl;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.queue.QueueEntry;
import org.apache.qpid.server.queue.QueueEntryVisitor;

public class ReportRunner<T>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ReportRunner.class);

    private static final Set<Class> IMMUTABLE_CLASSES = new HashSet<>(Arrays.<Class>asList(
            Boolean.class,
            Byte.class,
            Short.class,
            Character.class,
            Integer.class,
            Long.class,
            Float.class,
            Double.class,
            UUID.class,
            String.class
                                                                                          ));

    private ReportRunner(final QueueReport<T> report)
    {
        _report = report;
    }

    public boolean isBinaryReport()
    {
        return _report instanceof QueueBinaryReport;
    }

    public static ReportRunner<?> createRunner(final String reportName, final Map<String, String[]> parameterMap)
    {
        QueueReport<?> report = getReport(reportName);
        setReportParameters(report, parameterMap);
        return new ReportRunner<>(report);
    }

    private static void setReportParameters(final QueueReport<?> report, final Map<String, String[]> parameterMap)
    {
        if(parameterMap != null && !parameterMap.isEmpty())
        {
            Class<? extends QueueReport> clazz = report.getClass();
            for(Map.Entry<String,String[]> entry : parameterMap.entrySet())
            {
                String key = entry.getKey();
                String[] value = entry.getValue();
                if(isValidName(key))
                {

                    StringBuilder setterName = new StringBuilder("set");
                    setterName.append(key.substring(0,1).toUpperCase());
                    if(key.length()>1)
                    {
                        setterName.append(key.substring(1));
                    }
                    Method method = null;
                    try
                    {

                        if (value == null || value.length == 0 || value.length == 1)
                        {
                            try
                            {
                                method = clazz.getMethod(setterName.toString(), String.class);
                                method.invoke(report, value == null || value.length == 0 ? null : value[0]);
                            }
                            catch (NoSuchMethodException | IllegalAccessException e)
                            {
                                method = null;
                            }
                        }
                        if (method == null)
                        {
                            try
                            {
                                method = clazz.getMethod(setterName.toString(), String[].class);
                                method.invoke(report, new Object[] { value });
                            }
                            catch (NoSuchMethodException | IllegalAccessException e)
                            {
                                LOGGER.info("Unknown parameter '"
                                            + key
                                            + "' (no setter) for report "
                                            + report.getName());
                            }
                        }
                    }
                    catch (InvocationTargetException e)
                    {
                        LOGGER.info("Error setting parameter '" + key + "' for report " + report.getName(), e);
                    }
                }
                else
                {
                    LOGGER.info("Invalid parameter name '" + key + "' running report " + report.getName());
                }
            }
        }
    }

    private static boolean isValidName(final String key)
    {
        if(key != null && key.length() != 0)
        {
            if(Character.isJavaIdentifierStart(key.charAt(0)))
            {
                for(int i = 1; i < key.length(); i++)
                {
                    if(!Character.isJavaIdentifierPart(key.charAt(i)))
                    {
                        return false;
                    }
                }
                return true;
            }

        }
        return false;

    }

    private static QueueReport<?> getReport(final String reportName)
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        for (final QueueReport report : ServiceLoader.load(QueueReport.class, classLoader))
        {
            if (report.getName().equals(reportName))
            {
                try
                {
                    return report.getClass().getDeclaredConstructor().newInstance();
                }
                catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                        NoSuchMethodException e)
                {
                    // can't happen as by definition must have public noargs constructor
                }
            }
        }
        throw new IllegalArgumentException("Unknown report: " + reportName);
    }

    public String getContentType()
    {
        return _report.getContentType();
    }


    private static class ReportVisitor implements QueueEntryVisitor
    {

        private final QueueReport _report;

        public ReportVisitor(final QueueReport report)
        {
            _report = report;
        }

        @Override
        public boolean visit(final QueueEntry entry)
        {
            _report.addMessage(convertMessage(entry));
            return _report.isComplete();
        }


    }


    private static ReportableMessage convertMessage(QueueEntry entry)
    {
        final MessageInfoImpl messageInfo = new MessageInfoImpl(entry, true);
        ServerMessage message = entry.getMessage();
        byte[] content;
        try (QpidByteBuffer contentBuffer = message.getContent())
        {
            content = new byte[contentBuffer.remaining()];
            contentBuffer.get(content);
        }

        return new ReportableMessage()
        {
            @Override
            public String getInitialRoutingAddress()
            {
                return messageInfo.getInitialRoutingAddress();
            }

            @Override
            public ReportableMessageHeader getMessageHeader()
            {
                return convertMessageHeader(messageInfo);
            }

            @Override
            public ByteBuffer getContent()
            {
                return ByteBuffer.wrap(content).asReadOnlyBuffer();
            }

            @Override
            public boolean isPersistent()
            {
                return messageInfo.isPersistent();
            }

            @Override
            public long getSize()
            {
                return messageInfo.getSize();
            }

            @Override
            public Date getExpiration()
            {
                return messageInfo.getExpirationTime();
            }

            @Override
            public long getMessageNumber()
            {
                return messageInfo.getId();
            }

            @Override
            public Date getArrivalTime()
            {
                return messageInfo.getArrivalTime();
            }
        };
    }

    private static ReportableMessageHeader convertMessageHeader(final MessageInfoImpl messageInfo)
    {
        return new ReportableMessageHeader()
        {
            @Override
            public String getCorrelationId()
            {
                return messageInfo.getCorrelationId();
            }

            @Override
            public Date getExpiration()
            {
                return messageInfo.getExpirationTime();
            }

            @Override
            public String getUserId()
            {
                return messageInfo.getUserId();
            }

            @Override
            public String getAppId()
            {
                return messageInfo.getApplicationId();
            }

            @Override
            public String getMessageId()
            {
                return messageInfo.getMessageId();
            }

            @Override
            public String getMimeType()
            {
                return messageInfo.getMimeType();
            }

            @Override
            public String getEncoding()
            {
                return messageInfo.getEncoding();
            }

            @Override
            public byte getPriority()
            {
                return (byte) messageInfo.getPriority();
            }

            @Override
            public Date getTimestamp()
            {
                return messageInfo.getTimestamp();
            }

            @Override
            public String getType()
            {
                return messageInfo.getType();
            }

            @Override
            public String getReplyTo()
            {
                return messageInfo.getReplyTo();
            }

            @Override
            public Object getHeader(final String name)
            {
                return makeImmutable(messageInfo.getHeaders().get(name));
            }

            @Override
            public boolean containsHeaders(final Set<String> names)
            {
                return messageInfo.getHeaders().keySet().contains(names);
            }

            @Override
            public boolean containsHeader(final String name)
            {
                return messageInfo.getHeaders().containsKey(name);
            }

            @Override
            public Collection<String> getHeaderNames()
            {
                return Collections.unmodifiableCollection(messageInfo.getHeaders().keySet());
            }
        };
    }

    private static Object makeImmutable(final Object value)
    {
        if(value == null || IMMUTABLE_CLASSES.contains(value.getClass()))
        {
            return value;
        }
        else if(value instanceof byte[])
        {
            return ByteBuffer.wrap((byte[])value).asReadOnlyBuffer();
        }
        else if(value instanceof List)
        {
            List orig = (List) value;
            List<Object> copy = new ArrayList<>(orig.size());
            for(Object element : orig)
            {
                copy.add(makeImmutable(element));
            }
            return copy;
        }
        else if(value instanceof Map)
        {
            Map<?,?> orig = (Map<?,?>) value;
            LinkedHashMap<Object,Object> copy = new LinkedHashMap<>();
            for(Map.Entry<?,?> entry : orig.entrySet())
            {
                copy.put(makeImmutable(entry.getKey()),makeImmutable(entry.getValue()));
            }
            return copy;
        }
        else if(value instanceof Date)
        {
            return new Date(((Date) value).getTime());
        }
        else return null;
    }

    private final QueueReport<T> _report;

    public final T runReport(Queue<?> queue)
    {
        _report.setQueue(queue);
        ReportVisitor visitor = new ReportVisitor(_report);
        queue.visit(visitor);
        return _report.getReport();
    }
}
