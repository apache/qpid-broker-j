/**
 *
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
package org.apache.qpid.server.filter;
//
// Based on like named file from r450141 of the Apache ActiveMQ project <http://www.activemq.org/site/home.html>
//

/**
 * A filter performing a comparison of two objects
 */
public abstract class LogicExpression<T> extends BinaryExpression<T> implements BooleanExpression<T>
{

    public static <E> BooleanExpression<E> createOR(BooleanExpression<E> lvalue, BooleanExpression<E> rvalue)
    {
        return new OrExpression<>(lvalue, rvalue);
    }

    public static <E> BooleanExpression<E> createAND(BooleanExpression<E> lvalue, BooleanExpression<E> rvalue)
    {
        return new AndExpression<>(lvalue, rvalue);
    }

    public LogicExpression(BooleanExpression<T> left, BooleanExpression<T> right)
    {
        super(left, right);
    }

    @Override
    public abstract Object evaluate(T message);

    @Override
    public boolean matches(T message)
    {
        Object object = evaluate(message);

        return (object != null) && (object == Boolean.TRUE);
    }

    private static class OrExpression<E> extends LogicExpression<E>
    {
        public OrExpression(final BooleanExpression<E> lvalue, final BooleanExpression<E> rvalue)
        {
            super(lvalue, rvalue);
        }

        @Override
        public Object evaluate(E message)
        {

            Boolean lv = (Boolean) getLeft().evaluate(message);
            // Can we do an OR shortcut??
            if ((lv != null) && lv.booleanValue())
            {
                return Boolean.TRUE;
            }

            Boolean rv = (Boolean) getRight().evaluate(message);

            return (rv == null) ? null : rv;
        }

        @Override
        public String getExpressionSymbol()
        {
            return "OR";
        }
    }

    private static class AndExpression<E> extends LogicExpression<E>
    {
        public AndExpression(final BooleanExpression<E> lvalue, final BooleanExpression<E> rvalue)
        {
            super(lvalue, rvalue);
        }

        @Override
        public Object evaluate(E message)
        {

            Boolean lv = (Boolean) getLeft().evaluate(message);

            // Can we do an AND shortcut??
            if (lv == null)
            {
                return null;
            }

            if (!lv.booleanValue())
            {
                return Boolean.FALSE;
            }

            Boolean rv = (Boolean) getRight().evaluate(message);

            return (rv == null) ? null : rv;
        }

        @Override
        public String getExpressionSymbol()
        {
            return "AND";
        }
    }
}
