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

import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;

/**
 * A filter performing a comparison of two objects
 */
public abstract class ComparisonExpression<T> extends BinaryExpression<T> implements BooleanExpression<T>
{

    public static <E> BooleanExpression<E> createBetween(Expression<E> value, Expression<E> left, Expression<E> right)
    {
        return LogicExpression.createAND(createGreaterThanEqual(value, left), createLessThanEqual(value, right));
    }

    public static <E> BooleanExpression<E> createNotBetween(Expression<E> value, Expression<E> left, Expression<E> right)
    {
        return LogicExpression.createOR(createLessThan(value, left), createGreaterThan(value, right));
    }

    private static final HashSet<Character> REGEXP_CONTROL_CHARS = new HashSet<Character>();

    static
    {
        REGEXP_CONTROL_CHARS.add('.');
        REGEXP_CONTROL_CHARS.add('\\');
        REGEXP_CONTROL_CHARS.add('[');
        REGEXP_CONTROL_CHARS.add(']');
        REGEXP_CONTROL_CHARS.add('^');
        REGEXP_CONTROL_CHARS.add('$');
        REGEXP_CONTROL_CHARS.add('?');
        REGEXP_CONTROL_CHARS.add('*');
        REGEXP_CONTROL_CHARS.add('+');
        REGEXP_CONTROL_CHARS.add('{');
        REGEXP_CONTROL_CHARS.add('}');
        REGEXP_CONTROL_CHARS.add('|');
        REGEXP_CONTROL_CHARS.add('(');
        REGEXP_CONTROL_CHARS.add(')');
        REGEXP_CONTROL_CHARS.add(':');
        REGEXP_CONTROL_CHARS.add('&');
        REGEXP_CONTROL_CHARS.add('<');
        REGEXP_CONTROL_CHARS.add('>');
        REGEXP_CONTROL_CHARS.add('=');
        REGEXP_CONTROL_CHARS.add('!');
    }

    static class LikeExpression<E> extends UnaryExpression<E> implements BooleanExpression<E>
    {

        private Pattern likePattern;

        public LikeExpression(Expression<E> right, String like, int escape)
        {
            super(right);

            StringBuilder regexp = new StringBuilder(like.length() * 2);
            regexp.append("\\A"); // The beginning of the input
            for (int i = 0; i < like.length(); i++)
            {
                char c = like.charAt(i);
                if (escape == (0xFFFF & c))
                {
                    i++;
                    if (i >= like.length())
                    {
                        // nothing left to escape...
                        break;
                    }

                    char t = like.charAt(i);
                    regexp.append("\\x");
                    regexp.append(Integer.toHexString(0xFFFF & t));
                }
                else if (c == '%')
                {
                    regexp.append(".*?"); // Do a non-greedy match
                }
                else if (c == '_')
                {
                    regexp.append("."); // match one
                }
                else if (REGEXP_CONTROL_CHARS.contains(c))
                {
                    regexp.append("\\x");
                    regexp.append(Integer.toHexString(0xFFFF & c));
                }
                else
                {
                    regexp.append(c);
                }
            }

            regexp.append("\\z"); // The end of the input

            likePattern = Pattern.compile(regexp.toString(), Pattern.DOTALL);
        }

        /**
         *  org.apache.activemq.filter.UnaryExpression#getExpressionSymbol()
         */
        @Override
        public String getExpressionSymbol()
        {
            return "LIKE";
        }

        /**
         *  org.apache.activemq.filter.Expression#evaluate(MessageEvaluationContext)
         */
        @Override
        public Object evaluate(E message)
        {

            Object rv = this.getRight().evaluate(message);

            if (rv == null)
            {
                return null;
            }

            if (!(rv instanceof String))
            {
                return
                    Boolean.FALSE;
            }

            return likePattern.matcher((String) rv).matches() ? Boolean.TRUE : Boolean.FALSE;
        }

        @Override
        public boolean matches(E message)
        {
            Object object = evaluate(message);

            return (object != null) && (object == Boolean.TRUE);
        }
    }

    public static <E> BooleanExpression<E> createLike(Expression<E> left, String right, String escape)
    {
        if ((escape != null) && (escape.length() != 1))
        {
            throw new SelectorParsingException(
                "The ESCAPE string literal is invalid.  It can only be one character.  Litteral used: " + escape);
        }

        int c = -1;
        if (escape != null)
        {
            c = 0xFFFF & escape.charAt(0);
        }

        return new LikeExpression<>(left, right, c);
    }

    public static <E> BooleanExpression<E> createNotLike(Expression<E> left, String right, String escape)
    {
        return UnaryExpression.createNOT(createLike(left, right, escape));
    }

    public static <E> BooleanExpression<E> createInFilter(Expression<E> left, List<?> elements, boolean allowNonJms)
    {

        if (!(allowNonJms || left instanceof PropertyExpression))
        {
            throw new SelectorParsingException("Expected a property for In expression, got: " + left);
        }

        return UnaryExpression.createInExpression(left, elements, false, allowNonJms);

    }

    public static <E> BooleanExpression<E> createNotInFilter(Expression<E> left, List<?> elements, boolean allowNonJms)
    {

        if (!(allowNonJms || left instanceof PropertyExpression))
        {
            throw new SelectorParsingException("Expected a property for In expression, got: " + left);
        }

        return UnaryExpression.createInExpression(left, elements, true, allowNonJms);

    }

    public static <E> BooleanExpression<E> createIsNull(Expression<E> left)
    {
        return doCreateEqual(left, ConstantExpression.<E>NULL());
    }

    public static <E> BooleanExpression<E> createIsNotNull(Expression<E> left)
    {
        return UnaryExpression.createNOT(doCreateEqual(left, ConstantExpression.<E>NULL()));
    }

    public static <E> BooleanExpression<E> createNotEqual(Expression<E> left, Expression<E> right)
    {
        return UnaryExpression.createNOT(createEqual(left, right));
    }

    public static <E> BooleanExpression<E> createEqual(Expression<E> left, Expression<E> right)
    {
        checkEqualOperand(left);
        checkEqualOperand(right);
        checkEqualOperandCompatability(left, right);

        return doCreateEqual(left, right);
    }

    private static <E> BooleanExpression<E> doCreateEqual(Expression<E> left, Expression<E> right)
    {
        return new EqualExpression<>(left, right);
    }

    public static <E> BooleanExpression<E> createGreaterThan(final Expression<E> left, final Expression<E> right)
    {
        checkLessThanOperand(left);
        checkLessThanOperand(right);

        return new ComparisonExpression<E>(left, right)
            {
                @Override
                protected boolean asBoolean(int answer)
                {
                    return answer > 0;
                }

                @Override
                public String getExpressionSymbol()
                {
                    return ">";
                }
            };
    }

    public static <E> BooleanExpression<E> createGreaterThanEqual(final Expression<E> left, final Expression<E> right)
    {
        checkLessThanOperand(left);
        checkLessThanOperand(right);

        return new ComparisonExpression<E>(left, right)
            {
                @Override
                protected boolean asBoolean(int answer)
                {
                    return answer >= 0;
                }

                @Override
                public String getExpressionSymbol()
                {
                    return ">=";
                }
            };
    }

    public static <E> BooleanExpression<E> createLessThan(final Expression<E> left, final Expression<E> right)
    {
        checkLessThanOperand(left);
        checkLessThanOperand(right);

        return new ComparisonExpression<E>(left, right)
            {

                @Override
                protected boolean asBoolean(int answer)
                {
                    return answer < 0;
                }

                @Override
                public String getExpressionSymbol()
                {
                    return "<";
                }

            };
    }

    public static <E> BooleanExpression<E> createLessThanEqual(final Expression<E> left, final Expression<E> right)
    {
        checkLessThanOperand(left);
        checkLessThanOperand(right);

        return new ComparisonExpression<E>(left, right)
            {

                @Override
                protected boolean asBoolean(int answer)
                {
                    return answer <= 0;
                }

                @Override
                public String getExpressionSymbol()
                {
                    return "<=";
                }
            };
    }

    /**
     * Only Numeric expressions can be used in {@literal >, >=, < or <=} expressions.
     *
     * @param expr expression to check
     */
    public static <E> void checkLessThanOperand(Expression<E> expr)
    {
        if (expr instanceof ConstantExpression)
        {
            Object value = ((ConstantExpression) expr).getValue();
            if (value instanceof Number)
            {
                return;
            }

            // Else it's boolean or a String..
            throw new SelectorParsingException("Value '" + expr + "' cannot be compared.");
        }

        if (expr instanceof BooleanExpression)
        {
            throw new SelectorParsingException("Value '" + expr + "' cannot be compared.");
        }
    }

    /**
     * Validates that the expression can be used in {@literal == or <>} expression.
     * Cannot not be NULL TRUE or FALSE literals.
     *
     * @param expr expression to check
     */
    public static <E> void checkEqualOperand(Expression<E> expr)
    {
        if (expr instanceof ConstantExpression)
        {
            Object value = ((ConstantExpression) expr).getValue();
            if (value == null)
            {
                throw new SelectorParsingException("'" + expr + "' cannot be compared.");
            }
        }
    }

    private static <E> void checkEqualOperandCompatability(Expression<E> left, Expression<E> right)
    {
        if ((left instanceof ConstantExpression) && (right instanceof ConstantExpression))
        {
            if ((left instanceof BooleanExpression) && !(right instanceof BooleanExpression))
            {
                throw new SelectorParsingException("'" + left + "' cannot be compared with '" + right + "'");
            }
        }
    }

    public ComparisonExpression(Expression<T> left, Expression<T> right)
    {
        super(left, right);
    }

    @Override
    public Object evaluate(T message)
    {
        Comparable lv = (Comparable) getLeft().evaluate(message);
        if (lv == null)
        {
            return null;
        }

        Comparable rv = (Comparable) getRight().evaluate(message);
        if (rv == null)
        {
            return null;
        }

        return compare(lv, rv);
    }

    protected Boolean compare(Comparable lv, Comparable rv)
    {
        Class lc = lv.getClass();
        Class rc = rv.getClass();
        // If the the objects are not of the same type,
        // try to convert up to allow the comparison.
        if (lc != rc)
        {
            if (lc == Byte.class)
            {
                if (rc == Short.class)
                {
                    lv = ((Number) lv).shortValue();
                }
                else if (rc == Integer.class)
                {
                    lv = ((Number) lv).intValue();
                }
                else if (rc == Long.class)
                {
                    lv = ((Number) lv).longValue();
                }
                else if (rc == Float.class)
                {
                    lv = ((Number) lv).floatValue();
                }
                else if (rc == Double.class)
                {
                    lv = ((Number) lv).doubleValue();
                }
                else
                {
                    return Boolean.FALSE;
                }
            }
            else if (lc == Short.class)
            {
                if (rc == Integer.class)
                {
                    lv = ((Number) lv).intValue();
                }
                else if (rc == Long.class)
                {
                    lv = ((Number) lv).longValue();
                }
                else if (rc == Float.class)
                {
                    lv = ((Number) lv).floatValue();
                }
                else if (rc == Double.class)
                {
                    lv = ((Number) lv).doubleValue();
                }
                else
                {
                    return Boolean.FALSE;
                }
            }
            else if (lc == Integer.class)
            {
                if (rc == Long.class)
                {
                    lv = ((Number) lv).longValue();
                }
                else if (rc == Float.class)
                {
                    lv = ((Number) lv).floatValue();
                }
                else if (rc == Double.class)
                {
                    lv = ((Number) lv).doubleValue();
                }
                else
                {
                    return Boolean.FALSE;
                }
            }
            else if (lc == Long.class)
            {
                if (rc == Integer.class)
                {
                    rv = ((Number) rv).longValue();
                }
                else if (rc == Float.class)
                {
                    lv = ((Number) lv).floatValue();
                }
                else if (rc == Double.class)
                {
                    lv = ((Number) lv).doubleValue();
                }
                else
                {
                    return Boolean.FALSE;
                }
            }
            else if (lc == Float.class)
            {
                if (rc == Integer.class)
                {
                    rv = ((Number) rv).floatValue();
                }
                else if (rc == Long.class)
                {
                    rv = ((Number) rv).floatValue();
                }
                else if (rc == Double.class)
                {
                    lv = ((Number) lv).doubleValue();
                }
                else
                {
                    return Boolean.FALSE;
                }
            }
            else if (lc == Double.class)
            {
                if (rc == Integer.class)
                {
                    rv = ((Number) rv).doubleValue();
                }
                else if (rc == Long.class)
                {
                    rv = ((Number) rv).doubleValue();
                }
                else if (rc == Float.class)
                {
                    rv = ((Number) rv).doubleValue();
                }
                else
                {
                    return Boolean.FALSE;
                }
            }
            else if (lv instanceof Enum)
            {
                if (rv instanceof String)
                {
                    try
                    {
                        rv = Enum.valueOf(lc, (String) rv);
                    }
                    catch (IllegalArgumentException e)
                    {
                        return Boolean.FALSE;
                    }
                }
                else
                {
                    return Boolean.FALSE;
                }
            }
            else if (lv instanceof String)
            {
                if (rv instanceof Enum)
                {
                    lv = Enum.valueOf(rc, (String) lv);
                }
                else
                {
                    return Boolean.FALSE;
                }
            }
            else
            {
                return Boolean.FALSE;
            }
        }

        return asBoolean(lv.compareTo(rv)) ? Boolean.TRUE : Boolean.FALSE;
    }

    protected abstract boolean asBoolean(int answer);

    @Override
    public boolean matches(T message)
    {
        Object object = evaluate(message);

        return (object != null) && (object == Boolean.TRUE);
    }

    private static class EqualExpression<E> extends ComparisonExpression<E>
    {
        public EqualExpression(final Expression<E> left, final Expression<E> right)
        {
            super(left, right);
        }

        @Override
        public Object evaluate(E message)
        {
            Object lv = getLeft().evaluate(message);
            Object rv = getRight().evaluate(message);

            // Iff one of the values is null
            if ((lv == null) ^ (rv == null))
            {
                return Boolean.FALSE;
            }

            if ((lv == rv) || lv.equals(rv))
            {
                return Boolean.TRUE;
            }

            if ((lv instanceof Comparable) && (rv instanceof Comparable))
            {
                return compare((Comparable) lv, (Comparable) rv);
            }

            return Boolean.FALSE;
        }

        @Override
        protected boolean asBoolean(int answer)
        {
            return answer == 0;
        }

        @Override
        public String getExpressionSymbol()
        {
            return "=";
        }
    }
}
