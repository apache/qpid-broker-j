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
package org.apache.qpid.server.query.engine.exception;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.qpid.server.query.engine.evaluator.EvaluationContextHolder;

/**
 * Exception thrown during query evaluation
 */
public class QueryEvaluationException extends QueryEngineException
{
    protected QueryEvaluationException()
    {
        super();
    }

    protected QueryEvaluationException(String message)
    {
        super(message);
    }

    protected QueryEvaluationException(Throwable throwable)
    {
        super(throwable);
    }

    protected QueryEvaluationException(String message, Throwable throwable)
    {
        super(message, throwable);
    }

    public static <R> QueryEvaluationException invalidFunctionParameter(final String functionName, final R parameter)
    {
        final String parameterType = parameter == null ? "null" : parameter.getClass().getSimpleName();
        final String errorMessage = String.format(Errors.FUNCTION.PARAMETER_INVALID, functionName, parameterType);
        return new QueryEvaluationException(errorMessage);
    }

    public static QueryEvaluationException invalidFunctionParameters(final String functionName, final List<?> invalidArgs)
    {
        final List<String> types = invalidArgs.stream().map(Object::getClass).map(Class::getSimpleName).distinct().collect(Collectors.toList());
        final String errorMessage = String.format(Errors.FUNCTION.PARAMETERS_INVALID, functionName, types);
        return new QueryEvaluationException(errorMessage);
    }

    public static QueryEvaluationException emptyFunctionParameter(final String functionName, final int argNumber)
    {
        final String errorMessage = String.format(Errors.FUNCTION.PARAMETER_EMPTY, functionName, argNumber);
        return new QueryEvaluationException(errorMessage);
    }

    public static QueryEvaluationException functionParameterLessThanOne(final String functionName, final int argNumber)
    {
        final String errorMessage = String.format(Errors.FUNCTION.PARAMETER_LESS_THAN_1, functionName, argNumber);
        return new QueryEvaluationException(errorMessage);
    }

    public static QueryEvaluationException fieldNotFound(String property)
    {
        final List<String> domains = EvaluationContextHolder.getEvaluationContext().currentExecution().getDomains();
        if (domains.size() == 1)
        {
            return QueryEvaluationException.of(Errors.ACCESSOR.FIELD_NOT_FOUND_IN_DOMAIN, domains.get(0), property);
        }
        return QueryEvaluationException.of(Errors.ACCESSOR.FIELD_NOT_FOUND_IN_DOMAINS, domains, property);
    }

    public static QueryEvaluationException of(String message, Object... args)
    {
        return new QueryEvaluationException(String.format(message, args));
    }
}
