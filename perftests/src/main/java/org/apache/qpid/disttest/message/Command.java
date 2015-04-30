/*
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
package org.apache.qpid.disttest.message;


import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;

import org.apache.qpid.disttest.Visitor;
import org.apache.qpid.disttest.client.Client;
import org.apache.qpid.disttest.controller.Controller;

/**
 * A command sent between the {@link Controller} and a {@link Client}
 */
public abstract class Command
{
    private final CommandType type;

    public Command(final CommandType type)
    {
        this.type = type;
    }

    public CommandType getType()
    {
        return type;
    }

    public void accept(Visitor visitor)
    {
        visitor.visit(this);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder(getClass().getSimpleName());
        builder.append('[');
        if(Command.class.isAssignableFrom(getClass().getSuperclass()))
        {
            builder.append(super.toString());
        }
        final Field[] fields = getClass().getDeclaredFields();
        AccessibleObject.setAccessible(fields, true);
        boolean first = true;
        for(Field field : fields)
        {
            if(first)
            {
                first = false;
            }
            else
            {
                builder.append(',');
            }
            builder.append(field.getName().replaceFirst("^_",""));
            builder.append('=');
            try
            {
                builder.append(field.get(this));
            }
            catch (IllegalAccessException e)
            {
                builder.append("<<exception>>");
            }
        }
        return builder.toString();
    }
}
