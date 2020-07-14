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
package org.apache.qpid.server.model;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static org.apache.qpid.server.model.Initialization.none;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ManagedAttribute
{
    boolean secure() default false;

    boolean mandatory() default false;

    boolean persist() default true;

    String defaultValue() default "";

    String description() default "";

    String[] validValues() default {};

    String validValuePattern() default "";

    boolean oversize() default false;

    String oversizedAltText() default "";

    String secureValueFilter() default "";

    /**
     * If true, the model attribute value cannot be mutated after construction.
     */
    boolean immutable() default false;

    Initialization initialization() default none;

    boolean updateAttributeDespiteUnchangedValue() default false;

    String validator() default "";
}
