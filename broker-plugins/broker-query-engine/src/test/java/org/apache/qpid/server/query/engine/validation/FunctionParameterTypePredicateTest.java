package org.apache.qpid.server.query.engine.validation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.UUID;

import org.junit.Test;

import org.apache.qpid.server.model.OverflowPolicy;
import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryValidationException;

public class FunctionParameterTypePredicateTest
{
    @Test()
    public void emptyPredicate()
    {
        try
        {
            FunctionParameterTypePredicate.builder().build();
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryValidationException.class, e.getClass());
            assertEquals(Errors.VALIDATION.FUNCTION_ARGS_PREDICATE_EMPTY, e.getMessage());
        }
    }

    @Test()
    public void allowDisallowBooleans()
    {
        try
        {
            FunctionParameterTypePredicate.builder().allowBooleans().disallowBooleans().build();
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryValidationException.class, e.getClass());
            assertEquals(Errors.VALIDATION.FUNCTION_ARGS_PREDICATE_EMPTY, e.getMessage());
        }
    }

    @Test()
    @SuppressWarnings("unchecked")
    public <R> void booleans()
    {
        FunctionParameterTypePredicate<R> predicate = FunctionParameterTypePredicate.<R>builder().allowBooleans().build();
        assertFalse(predicate.test(null));
        assertTrue(predicate.test((R) Boolean.TRUE));
        assertTrue(predicate.test((R) Boolean.FALSE));
        assertTrue(predicate.test((R) (Boolean) true));
        assertTrue(predicate.test((R) (Boolean) false));
        assertFalse(predicate.test((R) (Integer) 1));
        assertFalse(predicate.test((R) (Long) 1L));
        assertFalse(predicate.test((R) (Double) 1.0));
        assertFalse(predicate.test((R) BigDecimal.ONE));
        assertFalse(predicate.test((R) "true"));
        assertFalse(predicate.test((R) UUID.randomUUID()));
        assertFalse(predicate.test((R) OverflowPolicy.NONE));
        assertFalse(predicate.test((R) new Date()));
        assertFalse(predicate.test((R) Instant.now()));
        assertFalse(predicate.test((R) LocalDate.now()));
        assertFalse(predicate.test((R) LocalDateTime.now()));
    }

    @Test()
    @SuppressWarnings("unchecked")
    public <R> void numbers()
    {
        FunctionParameterTypePredicate<R> predicate = FunctionParameterTypePredicate.<R>builder().allowNumbers().build();
        assertFalse(predicate.test(null));
        assertFalse(predicate.test((R) Boolean.TRUE));
        assertFalse(predicate.test((R) Boolean.FALSE));
        assertFalse(predicate.test((R) (Boolean) true));
        assertFalse(predicate.test((R) (Boolean) false));
        assertTrue(predicate.test((R) (Integer) 1));
        assertTrue(predicate.test((R) (Long) 1L));
        assertTrue(predicate.test((R) (Double) 1.0));
        assertTrue(predicate.test((R) BigDecimal.ONE));
        assertFalse(predicate.test((R) "true"));
        assertFalse(predicate.test((R) UUID.randomUUID()));
        assertFalse(predicate.test((R) OverflowPolicy.NONE));
        assertFalse(predicate.test((R) new Date()));
        assertFalse(predicate.test((R) Instant.now()));
        assertFalse(predicate.test((R) LocalDate.now()));
        assertFalse(predicate.test((R) LocalDateTime.now()));
    }

    @Test()
    @SuppressWarnings("unchecked")
    public <R> void enums()
    {
        FunctionParameterTypePredicate<R> predicate = FunctionParameterTypePredicate.<R>builder().allowEnums().build();
        assertFalse(predicate.test(null));
        assertFalse(predicate.test((R) Boolean.TRUE));
        assertFalse(predicate.test((R) Boolean.FALSE));
        assertFalse(predicate.test((R) (Boolean) true));
        assertFalse(predicate.test((R) (Boolean) false));
        assertFalse(predicate.test((R) (Integer) 1));
        assertFalse(predicate.test((R) (Long) 1L));
        assertFalse(predicate.test((R) (Double) 1.0));
        assertFalse(predicate.test((R) BigDecimal.ONE));
        assertFalse(predicate.test((R) "true"));
        assertFalse(predicate.test((R) UUID.randomUUID()));
        assertTrue(predicate.test((R) OverflowPolicy.NONE));
        assertFalse(predicate.test((R) new Date()));
        assertFalse(predicate.test((R) Instant.now()));
        assertFalse(predicate.test((R) LocalDate.now()));
        assertFalse(predicate.test((R) LocalDateTime.now()));
    }

    @Test()
    @SuppressWarnings("unchecked")
    public <R> void nulls()
    {
        FunctionParameterTypePredicate<R> predicate = FunctionParameterTypePredicate.<R>builder().allowNulls().build();
        assertTrue(predicate.test(null));
        assertFalse(predicate.test((R) Boolean.TRUE));
        assertFalse(predicate.test((R) Boolean.FALSE));
        assertFalse(predicate.test((R) (Boolean) true));
        assertFalse(predicate.test((R) (Boolean) false));
        assertFalse(predicate.test((R) (Integer) 1));
        assertFalse(predicate.test((R) (Long) 1L));
        assertFalse(predicate.test((R) (Double) 1.0));
        assertFalse(predicate.test((R) BigDecimal.ONE));
        assertFalse(predicate.test((R) "true"));
        assertFalse(predicate.test((R) UUID.randomUUID()));
        assertFalse(predicate.test((R) OverflowPolicy.NONE));
        assertFalse(predicate.test((R) new Date()));
        assertFalse(predicate.test((R) Instant.now()));
        assertFalse(predicate.test((R) LocalDate.now()));
        assertFalse(predicate.test((R) LocalDateTime.now()));
    }

    @Test()
    @SuppressWarnings("unchecked")
    public <R> void strings()
    {
        FunctionParameterTypePredicate<R> predicate = FunctionParameterTypePredicate.<R>builder().allowStrings().build();
        assertFalse(predicate.test(null));
        assertFalse(predicate.test((R) Boolean.TRUE));
        assertFalse(predicate.test((R) Boolean.FALSE));
        assertFalse(predicate.test((R) (Boolean) true));
        assertFalse(predicate.test((R) (Boolean) false));
        assertFalse(predicate.test((R) (Integer) 1));
        assertFalse(predicate.test((R) (Long) 1L));
        assertFalse(predicate.test((R) (Double) 1.0));
        assertFalse(predicate.test((R) BigDecimal.ONE));
        assertTrue(predicate.test((R) "true"));
        assertFalse(predicate.test((R) UUID.randomUUID()));
        assertFalse(predicate.test((R) OverflowPolicy.NONE));
        assertFalse(predicate.test((R) new Date()));
        assertFalse(predicate.test((R) Instant.now()));
        assertFalse(predicate.test((R) LocalDate.now()));
        assertFalse(predicate.test((R) LocalDateTime.now()));
    }

    @Test()
    @SuppressWarnings("unchecked")
    public <R> void datetime()
    {
        FunctionParameterTypePredicate<R> predicate = FunctionParameterTypePredicate.<R>builder().allowDateTimeTypes().build();
        assertFalse(predicate.test(null));
        assertFalse(predicate.test((R) Boolean.TRUE));
        assertFalse(predicate.test((R) Boolean.FALSE));
        assertFalse(predicate.test((R) (Boolean) true));
        assertFalse(predicate.test((R) (Boolean) false));
        assertFalse(predicate.test((R) (Integer) 1));
        assertFalse(predicate.test((R) (Long) 1L));
        assertFalse(predicate.test((R) (Double) 1.0));
        assertFalse(predicate.test((R) BigDecimal.ONE));
        assertFalse(predicate.test((R) "true"));
        assertFalse(predicate.test((R) UUID.randomUUID()));
        assertFalse(predicate.test((R) OverflowPolicy.NONE));
        assertTrue(predicate.test((R) new Date()));
        assertTrue(predicate.test((R) Instant.now()));
        assertTrue(predicate.test((R) LocalDate.now()));
        assertTrue(predicate.test((R) LocalDateTime.now()));
    }

    @Test()
    @SuppressWarnings("unchecked")
    public <R> void comparables()
    {
        FunctionParameterTypePredicate<R> predicate = FunctionParameterTypePredicate.<R>builder().allowComparables().build();
        assertFalse(predicate.test(null));
        assertFalse(predicate.test((R) Boolean.TRUE));
        assertFalse(predicate.test((R) Boolean.FALSE));
        assertFalse(predicate.test((R) (Boolean) true));
        assertFalse(predicate.test((R) (Boolean) false));
        assertTrue(predicate.test((R) (Integer) 1));
        assertTrue(predicate.test((R) (Long) 1L));
        assertTrue(predicate.test((R) (Double) 1.0));
        assertTrue(predicate.test((R) BigDecimal.ONE));
        assertTrue(predicate.test((R) "true"));
        assertTrue(predicate.test((R) UUID.randomUUID()));
        assertTrue(predicate.test((R) OverflowPolicy.NONE));
        assertTrue(predicate.test((R) new Date()));
        assertTrue(predicate.test((R) Instant.now()));
        assertTrue(predicate.test((R) LocalDate.now()));
        assertTrue(predicate.test((R) LocalDateTime.now()));
    }
}
