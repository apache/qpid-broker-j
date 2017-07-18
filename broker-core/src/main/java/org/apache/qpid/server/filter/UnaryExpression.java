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
package org.apache.qpid.server.filter;
//
// Based on like named file from r450141 of the Apache ActiveMQ project <http://www.activemq.org/site/home.html>
//

import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/**
 * An expression which performs an operation on two expression values
 */
public abstract class UnaryExpression<T> implements Expression<T>
{

    private static final BigDecimal BD_LONG_MIN_VALUE = BigDecimal.valueOf(Long.MIN_VALUE);
    private Expression<T> right;

    public static <E> Expression<E> createNegate(Expression<E> left)
    {
        return new NegativeExpression<>(left);
    }

    public static <E> BooleanExpression<E> createInExpression(Expression<E> right,
                                                              List<?> elements,
                                                              final boolean not,
                                                              final boolean allowNonJms)
    {

        // Use a HashSet if there are many elements.
        Collection<?> t;
        if (elements.size() == 0)
        {
            t = null;
        }
        else if (elements.size() < 5)
        {
            t = elements;
        }
        else
        {
            t = new HashSet<>(elements);
        }

        final Collection<?> inList = t;

        return new InExpression<>(right, inList, not, allowNonJms);
    }

    abstract static class BooleanUnaryExpression<E> extends UnaryExpression<E> implements BooleanExpression<E>
    {
        public BooleanUnaryExpression(Expression<E> left)
        {
            super(left);
        }

        @Override
        public boolean matches(E message)
        {
            Object object = evaluate(message);

            return (object != null) && (object == Boolean.TRUE);
        }
    }

    public static <E> BooleanExpression<E> createNOT(BooleanExpression<E> left)
    {
        return new NotExpression<>(left);
    }

    public static <E> BooleanExpression<E> createBooleanCast(Expression<E> left)
    {
        return new BooleanCastExpression<>(left);
    }

    private static Number negate(Number left)
    {
        Class clazz = left.getClass();
        if (clazz == Integer.class)
        {
            return -left.intValue();
        }
        else if (clazz == Long.class)
        {
            return -left.longValue();
        }
        else if (clazz == Float.class)
        {
            return -left.floatValue();
        }
        else if (clazz == Double.class)
        {
            return -left.doubleValue();
        }
        else if (clazz == BigDecimal.class)
        {
            // We usually get a big decimal when we have Long.MIN_VALUE constant in the
            // Selector.  Long.MIN_VALUE is too big to store in a Long as a positive so we store it
            // as a Big decimal.  But it gets Negated right away.. to here we try to covert it back
            // to a Long.
            BigDecimal bd = (BigDecimal) left;
            bd = bd.negate();

            if (BD_LONG_MIN_VALUE.compareTo(bd) == 0)
            {
                return Long.MIN_VALUE;
            }

            return bd;
        }
        else
        {
            throw new SelectorParsingException("Don't know how to negate: " + left);
        }
    }

    public UnaryExpression(Expression<T> left)
    {
        this.right = left;
    }

    public Expression<T> getRight()
    {
        return right;
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        return "(" + getExpressionSymbol() + " " + right.toString() + ")";
    }

    /**
     * TODO: more efficient hashCode()
     *
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode()
    {
        return toString().hashCode();
    }

    /**
     * TODO: more efficient hashCode()
     *
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object o)
    {
        return ((o != null) && this.getClass().equals(o.getClass())) && toString().equals(o.toString());
    }

    /**
     * Returns the symbol that represents this binary expression.  For example, addition is
     * represented by "+"
     *
     * @return symbol
     */
    public abstract String getExpressionSymbol();

    private static class NegativeExpression<E> extends UnaryExpression<E>
    {
        public NegativeExpression(final Expression<E> left)
        {
            super(left);
        }

        @Override
        public Object evaluate(E message)
        {
            Object rvalue = getRight().evaluate(message);
            if (rvalue == null)
            {
                return null;
            }

            if (rvalue instanceof Number)
            {
                return negate((Number) rvalue);
            }

            return null;
        }

        @Override
        public String getExpressionSymbol()
        {
            return "-";
        }
    }

    private static class InExpression<E> extends BooleanUnaryExpression<E>
    {
        private final Collection<?> _inList;
        private final boolean _not;
        private final boolean _allowNonJms;

        public InExpression(final Expression<E> right,
                            final Collection<?> inList,
                            final boolean not,
                            final boolean allowNonJms)
        {
            super(right);
            _inList = inList;
            _not = not;
            _allowNonJms = allowNonJms;
        }

        @Override
        public Object evaluate(E expression)
        {

            Object rvalue = getRight().evaluate(expression);
            if (rvalue == null || !(_allowNonJms || rvalue instanceof String))
            {
                return null;
            }

            if (((_inList != null) && isInList(rvalue, expression)) ^ _not)
            {
                return Boolean.TRUE;
            }
            else
            {
                return Boolean.FALSE;
            }

        }

        private boolean isInList(final Object rvalue, final E expression)
        {
            for(Object entry : _inList)
            {
                Object currentRvalue = rvalue;
                Object listItemValue = entry instanceof Expression ? ((Expression<E>)entry).evaluate(expression) : entry;
                if (currentRvalue instanceof Enum && listItemValue instanceof String)
                {
                    listItemValue = convertStringToEnumValue(currentRvalue.getClass(), (String) listItemValue);
                }
                if (listItemValue instanceof Enum && currentRvalue instanceof String)
                {
                    currentRvalue = convertStringToEnumValue(listItemValue.getClass(), (String) currentRvalue);
                }

                if((currentRvalue == null && listItemValue == null) || (currentRvalue != null && currentRvalue.equals(listItemValue)))
                {
                    return true;
                }
                if(currentRvalue instanceof Number && listItemValue instanceof Number)
                {
                    Number num1 = (Number) currentRvalue;
                    Number num2 = (Number) listItemValue;
                    if (num1.doubleValue() == num2.doubleValue() && num1.longValue() == num2.longValue())
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        private Object convertStringToEnumValue(final Class<?> enumType, String candidateValue)
        {
            try
            {
                Class rclazz = enumType;
                return Enum.valueOf(rclazz, candidateValue);
            }
            catch (IllegalArgumentException iae)
            {
                return candidateValue;
            }
        }

        @Override
        public String toString()
        {
            StringBuilder answer = new StringBuilder(String.valueOf(getRight()));
            answer.append(" ");
            answer.append(getExpressionSymbol());
            answer.append(" ( ");

            int count = 0;
            for (Object o : _inList)
            {
                if (count != 0)
                {
                    answer.append(", ");
                }

                answer.append(o);
                count++;
            }

            answer.append(" )");

            return answer.toString();
        }

        @Override
        public String getExpressionSymbol()
        {
            if (_not)
            {
                return "NOT IN";
            }
            else
            {
                return "IN";
            }
        }
    }

    private static class NotExpression<E> extends BooleanUnaryExpression<E>
    {
        public NotExpression(final BooleanExpression<E> left)
        {
            super(left);
        }

        @Override
        public Object evaluate(E message)
        {
            Boolean lvalue = (Boolean) getRight().evaluate(message);
            if (lvalue == null)
            {
                return null;
            }

            return lvalue ? Boolean.FALSE : Boolean.TRUE;
        }

        @Override
        public String getExpressionSymbol()
        {
            return "NOT";
        }
    }

    private static class BooleanCastExpression<E> extends BooleanUnaryExpression<E>
    {
        public BooleanCastExpression(final Expression<E> left)
        {
            super(left);
        }

        @Override
        public Object evaluate(E message)
        {
            Object rvalue = getRight().evaluate(message);
            if (rvalue == null)
            {
                return null;
            }

            if (!rvalue.getClass().equals(Boolean.class))
            {
                return Boolean.FALSE;
            }

            return ((Boolean) rvalue) ? Boolean.TRUE : Boolean.FALSE;
        }

        @Override
        public String toString()
        {
            return getRight().toString();
        }

        @Override
        public String getExpressionSymbol()
        {
            return "";
        }
    }
}
