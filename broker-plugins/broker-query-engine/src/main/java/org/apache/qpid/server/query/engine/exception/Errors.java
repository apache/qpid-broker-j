package org.apache.qpid.server.query.engine.exception;


/**
 * Contains error messages for convenient access
 */
public final class Errors
{
    private Errors() { }

    public static final class ACCESSOR
    {
        private ACCESSOR() { }
        public static final String FIELD_NOT_FOUND_IN_DOMAIN = "Domain '%s' does not contain field '%s'";
        public static final String FIELD_NOT_FOUND_IN_DOMAINS = "Domains %s do not contain field '%s'";
        public static final String UNABLE_TO_ACCESS = "Field '%s' inaccessible: %s";
    }

    public static final class AGGREGATION
    {
        private AGGREGATION() { }
        public static final String DISTINCT_NOT_FOUND = "Function COUNT first argument must be 'distinct'";
        public static final String MISUSED = "%s.apply() should be called after calling aggregate()";
    }

    public static final class ARITHMETIC
    {
        private ARITHMETIC() { }
        public static final String NOT_SUPPORTED = "Operator '%s' doesn't support arguments '%s' of type '%s' and '%s' of type '%s'";
        public static final String ZERO_DIVISION = "Divisor is equal to zero";
    }

    public static final class COMPARISON
    {
        private COMPARISON() { }
        public static final String INAPPLICABLE = "Objects of types '%s' and '%s' can not be compared";
    }

    public static final class CONVERSION
    {
        private CONVERSION() { }
        public static final String FAILED = "Object of type '%s' can not be converted to %s";
        public static final String MAX_VALUE_REACHED = "Reached maximal allowed big decimal value: %s";
        public static final String NOT_SUPPORTED = "Conversion of value '%s' of type '%s' to target type '%s' not supported";
    }

    public static final class EVALUATION
    {
        private EVALUATION() { }
        public static final String BROKER_NOT_SUPPLIED = "Broker instance not provided for querying";
        public static final String CHILD_EXPRESSION_NOT_FOUND = "Child expression of parent '%s' with index %d not found";
        public static final String DEFAULT_QUERY_SETTINGS_NOT_SUPPLIED = "Default query settings not provided";
        public static final String EVALUATOR_NOT_SUPPLIED = "QueryEvaluator instance not supplied";
        public static final String QUERY_NOT_SUPPLIED = "Query not provided";
        public static final String QUERY_SETTINGS_NOT_SUPPLIED = "Query settings not provided";
    }

    public static final class FUNCTION
    {
        private FUNCTION() { }
        public static final String NOT_FOUND = "Function '%s' not found";
        public static final String DATEPART_NOT_SUPPORTED = "Datepart '%s' not supported";
        public static final String PARAMETER_EMPTY = "Function '%s' requires argument %d to be a non-empty string";
        public static final String PARAMETER_INVALID = "Parameter of function '%s' invalid (parameter type: %s)";
        public static final String PARAMETERS_INVALID = "Parameters of function '%s' invalid (invalid types: %s)";
        public static final String PARAMETER_LESS_THAN_1 = "Function '%s' requires argument %d to be an integer greater than 0";
        public static final String PARAMETER_NOT_INTEGER = "Function '%s' requires argument %d to be an integer";
        public static final String PARAMETER_NOT_STRING = "Function '%s' requires argument %d to be a string";
        public static final String REQUIRE_MAX_PARAMETERS = "Function '%s' requires maximum %d parameters";
        public static final String REQUIRE_MIN_PARAMETER = "Function '%s' requires at least %d parameter";
        public static final String REQUIRE_MIN_PARAMETERS = "Function '%s' requires at least %d parameters";
        public static final String REQUIRE_PARAMETER = "Function '%s' requires %d parameter";
        public static final String REQUIRE_PARAMETERS = "Function '%s' requires %d parameters";
    }

    public static final class NEGATION
    {
        private NEGATION() { }
        public static final String NOT_SUPPORTED = "Negation of '%s' not supported";
    }

    public static final class SET
    {
        private SET() { }
        public static final String PRODUCTS_HAVE_DIFFERENT_LENGTH = "Products of '%s' operation have different length";
    }

    public static final class VALIDATION
    {
        private VALIDATION() { }
        public static final String CASE_CONDITIONS_EMPTY = "List of case operator conditions should not be empty";
        public static final String CASE_OUTCOMES_EMPTY = "List of case operator outcomes should not be empty";
        public static final String CHILD_EXPRESSION_NULL = "Child expression should be not null";
        public static final String CHILD_EXPRESSIONS_NULL = "Child expressions should be not null";
        public static final String COLLECTOR_TYPE_NULL = "Collector type should be not null";
        public static final String DOMAIN_NOT_SUPPORTED = "Querying from '%s' not supported";
        public static final String FUNCTION_ARGS_NULL = "Function argument list should be not null";
        public static final String FUNCTION_ARGS_PREDICATE_EMPTY = "Function type validator empty, please supply some validation rules";
        public static final String FUNCTION_NAME_NULL = "Function name should be not null";
        public static final String GROUP_BY_ORDINAL_INVALID = "Group by item must be the number of a select list expression";
        public static final String HAVING_WITHOUT_AGGREGATION = "HAVING clause is allowed when using aggregation";
        public static final String INDEX_NULL = "Index value should be not null";
        public static final String INVALID_ORDINAL = "Ordinal value should be greater than 0";
        public static final String INVALID_SORT_FIELD = "Sorting by field '%s' not supported";
        public static final String JOINS_NOT_SUPPORTED = "Joins are not supported";
        public static final String KEYWORD_FROM_NOT_FOUND = "Keyword 'FROM' not found where expected";
        public static final String MAX_QUERY_DEPTH_REACHED = "Max query depth reached: %d";
        public static final String MISSING_EXPRESSION = "Missing expression";
        public static final String MULTIPLE_DOMAINS_NOT_SUPPORTED = "Querying from multiple domains not supported";
        public static final String NOT_A_SINGLE_GROUP_EXPRESSION = "Not a single-group group function: projections %s should be included in GROUP BY clause";
        public static final String ORDER_BY_ORDINAL_INVALID = "Order by item must be the number of a select list expression";
        public static final String PROPERTY_NAME_NULL = "Property name should be not null";
        public static final String QUERY_EMPTY = "Query should not be empty";
        public static final String QUERY_EXPRESSION_NULL = "Query expression must be not null";
        public static final String SELECT_EXPRESSION_NULL = "Select expression must be not null";
        public static final String SUBQUERY_RETURNS_MULTIPLE_ROWS = "Single-row subquery '%s' returns more than one row: %d";
        public static final String SUBQUERY_RETURNS_MULTIPLE_VALUES = "Subquery '%s' returns more than one value: %s";
        public static final String WITH_ITEM_COMLUMNS_INVALID = "Number of WITH clause column names does not match number of elements in select list";
    }
}
