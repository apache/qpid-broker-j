/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.qpid.server.management.plugin.servlet.rest;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.TypedContent;
import org.apache.qpid.server.model.VirtualHost;

public class MessageContentServlet extends AbstractServlet
{
    public MessageContentServlet()
    {
        super();
    }

    @Override
    protected void doGetWithSubjectAndActor(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException
    {
        String[] pathInfoElements = getPathInfoElements(request);
        if(pathInfoElements != null && pathInfoElements.length > 2)
        {
            getMessageContent(request, response);
        }

    }

    private void getMessageContent(HttpServletRequest request, HttpServletResponse response) throws IOException
    {
        Queue queue = getQueueFromRequest(request);
        String path[] = getPathInfoElements(request);
        TypedContent content = queue.getMessageContent(Long.parseLong(path[2]));
        if(content != null)
        {
            writeTypedContent(content, request, response);
        }
    }

    private Queue getQueueFromRequest(HttpServletRequest request)
    {
        // TODO - validation that there is a vhost and queue and only those in the path

        String[] pathInfoElements = getPathInfoElements(request);
        if(pathInfoElements == null || pathInfoElements.length < 2)
        {
            throw new IllegalArgumentException("Invalid path is specified");
        }
        String vhostName = pathInfoElements[0];
        String queueName = pathInfoElements[1];

        VirtualHost<?,?,?> vhost = getBroker().findVirtualHostByName(vhostName);
        if (vhost == null)
        {
            throw new IllegalArgumentException("Could not find virtual host with name '" + vhostName + "'");
        }
        return getQueueFromVirtualHost(queueName, vhost);
    }

    private Queue getQueueFromVirtualHost(String queueName, VirtualHost<?,?,?> vhost)
    {
        Queue queue = null;

        for(Queue q : vhost.getQueues())
        {
            if(q.getName().equals(queueName))
            {
                queue = q;
                break;
            }
        }
        return queue;
    }




}
