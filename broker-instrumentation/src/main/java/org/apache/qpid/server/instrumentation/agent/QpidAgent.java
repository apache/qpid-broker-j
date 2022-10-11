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
package org.apache.qpid.server.instrumentation.agent;

import java.lang.instrument.Instrumentation;

import org.apache.qpid.server.instrumentation.transformer.QpidClassFileTransformer;

/** Uses static instrumentation to modify broker classes before loading */
@SuppressWarnings("java:S1172")
public class QpidAgent
{
    private QpidAgent()
    {

    }

    public static void premain(final String args, final Instrumentation instrumentation)
    {
        instrumentation.addTransformer(new QpidClassFileTransformer(args, instrumentation));
    }
}
