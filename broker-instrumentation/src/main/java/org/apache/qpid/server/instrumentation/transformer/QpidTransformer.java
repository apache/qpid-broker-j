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
 */
package org.apache.qpid.server.instrumentation.transformer;

import java.util.List;

import org.objectweb.asm.ClassVisitor;
import org.slf4j.Logger;

import org.apache.qpid.server.instrumentation.metadata.MemberDescription;

/**
 * Class transformer
 *
 * @param <T> Class member
 */
public interface QpidTransformer<T extends MemberDescription>
{
    /**
     * Transforms class
     *
     * @param bytes Original class in form of a byte array
     * @return Transformed class in form of a byte array
     */
    byte[] generate(byte[] bytes);

    /**
     * Returns list of class members to be instrumented
     *
     * @return List of class members to be instrumented
     */
    List<T> getMemberDescriptions();

    /**
     * Returns delegate transformer
     *
     * @param cv ClassVisitor instance
     *
     * @return ClassVisitor instance
     */
    ClassVisitor getTransformer(final ClassVisitor cv);

    /**
     * Returns logger instance
     *
     * @return Logger instance
     */
    Logger getLogger();
}
