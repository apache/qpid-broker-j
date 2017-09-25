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

package org.apache.qpid.server.stats;

import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.TimerTask;

import javax.security.auth.Subject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.util.Strings;

public class StatisticsReportingTask extends TimerTask
{
    private final ConfiguredObject<?> _root;
    private final Subject _subject;

    public StatisticsReportingTask(final ConfiguredObject<?> root, final Subject subject)
    {
        _root = root;
        _subject = subject;
    }

    @Override
    public void run()
    {
        Subject.doAs(_subject, (PrivilegedAction<Object>) () -> {
            reportStatisticsForObject(_root);
            applyRecursively(_root);
            return null;
        });
    }

    private void applyRecursively(final ConfiguredObject<?> object)
    {
        Collection<Class<? extends ConfiguredObject>> childTypes = object.getModel().getChildTypes(object.getCategoryClass());
        childTypes.forEach(childClass -> processChildType(object, childClass));
    }

    private void processChildType(final ConfiguredObject<?> object, final Class<? extends ConfiguredObject> childType)
    {
        Collection<? extends ConfiguredObject> children = object.getChildren(childType);
        if (!children.isEmpty())
        {
            children.forEach(this::processChild);
        }
    }

    private void processChild(final ConfiguredObject<?> child)
    {
        reportStatisticsForObject(child);

        if (!child.getCategoryClass().getAnnotation(ManagedObject.class).managesChildren())
        {
            applyRecursively(child);
        }
    }

    private void reportStatisticsForObject(final ConfiguredObject<?> object)
    {
        final String statisticsReportPatternContextKey =
                String.format("qpid.%s.statisticsReportPattern",
                              object.getCategoryClass().getSimpleName().toLowerCase());

        if (object.getContextKeys(false).contains(statisticsReportPatternContextKey))
        {
            String statisticsReportPattern = object.getContextValue(String.class, statisticsReportPatternContextKey);

            String formattedStatistics = Strings.expand(statisticsReportPattern,
                                                        false,
                                                        new FormattingStatisticsResolver(object));

            String loggerName = String.format("qpid.statistics.%s", object.getCategoryClass().getSimpleName());
            Logger logger = LoggerFactory.getLogger(loggerName);

            logger.info("Statistics: {}", formattedStatistics);
        }
    }
}
