package org.apache.qpid.server.query.engine.parsing.expression.function.datetime;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import org.apache.qpid.server.query.engine.evaluator.EvaluationContext;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.parsing.converter.DateTimeConverter;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.function.AbstractFunctionExpression;
import org.apache.qpid.server.query.engine.validation.FunctionParametersValidator;
import org.apache.qpid.server.query.engine.validation.FunctionParameterTypePredicate;

/**
 * The DATE() function extracts the date part from a datetime expression.
 *
 * @param <T> Input parameter type
 * @param <R> Output parameter type
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class DateExpression<T, R> extends AbstractFunctionExpression<T, LocalDate>
{
    /**
     * Argument type validator
     */
    private final FunctionParameterTypePredicate<R> _dateTimeValidator = FunctionParameterTypePredicate.<R>builder()
        .allowDateTimeTypes()
        .allowNulls()
        .allowStrings()
        .build();

    /**
     * Constructor initializes children expression list
     *
     * @param alias Expression alias
     * @param args List of children expressions
     */
    public DateExpression(final String alias, final List<ExpressionNode<T, ?>> args)
    {
        super(alias, args);
        FunctionParametersValidator.requireParameters(1, args, this);
    }

    /**
     * Performs date operation using parameters and the value supplied
     *
     * @param value Object to handle
     *
     * @return Resulting date
     */
    @Override
    public LocalDate apply(final T value)
    {
        final QuerySettings querySettings = ctx().get(EvaluationContext.QUERY_SETTINGS);
        final R dateTime = evaluateChild(0, value, _dateTimeValidator);
        if (dateTime == null)
        {
            return null;
        }
        final Instant instant = DateTimeConverter.toInstantMapper().apply(dateTime);
        return instant.atZone(querySettings.getZoneId()).toLocalDate();
    }
}
