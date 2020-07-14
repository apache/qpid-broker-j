package org.apache.qpid.server.logging.logback.validator;

import org.apache.qpid.server.model.validator.TestConfiguredObject;
import org.apache.qpid.server.model.validator.ValueValidator;
import org.apache.qpid.test.utils.UnitTestBase;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class GelfMessageStaticFieldsValidatorTest extends UnitTestBase
{
    @Test
    public void testValidator()
    {
        assertNotNull("Factory method has to produce a instance", GelfMessageStaticFieldsValidator.validator());
    }

    @Test
    public void testIsValid_ValidInput()
    {
        ValueValidator validator = GelfMessageStaticFieldsValidator.validator();
        assertNotNull(validator);

        Map<String, Object> map = new HashMap<>();
        map.put("A.A", "A");
        map.put("B.B", 123);
        validator.isValid(map);
        validator.isValid(Collections.emptyMap());
    }

    @Test
    public void testIsValid_InValidInput()
    {
        ValueValidator validator = GelfMessageStaticFieldsValidator.validator();
        assertNotNull(validator);

        assertFalse(validator.isValid(Collections.singletonMap("_{}%", "A")));
        assertFalse(validator.isValid(Collections.singletonMap("B", Collections.emptyList())));
        assertFalse(validator.isValid("A"));
    }

    @Test
    public void testErrorMessage()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = GelfMessageStaticFieldsValidator.validator();
        assertNotNull(validator);

        String message = validator.errorMessage(Collections.singletonMap(123,"A"), object, "staticFields");
        assertEquals("Key of 'staticFields.key attribute instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot be '123', as it has to be a string", message);

        message = validator.errorMessage(Collections.singletonMap("{}%X","A"), object, "staticFields");
        assertEquals("Key of 'staticFields.key attribute instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot be '{}%X'. Key pattern is: [\\w\\.\\-]+", message);

        message = validator.errorMessage(Collections.singletonMap("A.A",Collections.singletonList(123)), object, "staticFields");
        assertEquals("Value of 'staticFields.value attribute instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot be '[123]', as it has to be a string or number", message);
    }
}