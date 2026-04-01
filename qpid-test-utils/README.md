# Module qpid-test-utils

This module provides utility classes and JUnit extensions used in Broker-J unit tests.

## QpidUnitTestExtension

`QpidUnitTestExtension` is a JUnit extension that logs test method execution start and end, and
routes log output into per-test files using Logback's `SiftingAppender`.

### How it works

The extension sets a `classQualifiedTestName` property on the Logback `LoggerContext` at each stage
of the test lifecycle. `LogbackPropertyValueDiscriminator` reads this property and the
`SiftingAppender` uses it to select the target log file.

| Lifecycle phase | Property value               | Log file                                |
|-----------------|------------------------------|-----------------------------------------|
| `@BeforeAll`    | `com.example.MyTest`         | `TEST-com.example.MyTest.txt`           |
| `@BeforeEach`   | `com.example.MyTest.myTest`  | `TEST-com.example.MyTest.myTest.txt`    |
| test execution  | `com.example.MyTest.myTest`  | `TEST-com.example.MyTest.myTest.txt`    |
| `@AfterEach`    | `com.example.MyTest`         | `TEST-com.example.MyTest.txt`           |
| `@AfterAll`     | cleared                      | `TEST-testrun.txt` (default)            |

Log files are written to `target/surefire-reports/` (configurable via the `test.output.dir` system
property).

### Design note: LogbackPropertyValueDiscriminator vs MDC

The extension sets a property on the Logback `LoggerContext` (a process-wide property map) rather
than using MDC. MDC values are thread-local and would be lost in background threads, producing
incorrect log routing. Since production code frequently performs logging in background threads, the
context-property approach ensures all log output from a test - regardless of which thread produces
it - is routed to the correct file.

### Example: using QpidUnitTestExtension

For tests that cannot extend `UnitTestBase`, apply the extension directly:

```java
@ExtendWith(QpidUnitTestExtension.class)
class MyTest
{
    @Test
    void testA()
    {
        // test logic ...
    }

    @Test
    void testB()
    {
        // test logic ...
    }
}
```

### Expected log files

Running the test class `org.example.MyTest` with methods `testA` and `testB` produces:

```
target/surefire-reports/
    TEST-org.example.MyTest.txt           # class-level log (beforeAll/afterAll, cleanup)
    TEST-org.example.MyTest.testA.txt     # method-level log for testA
    TEST-org.example.MyTest.testB.txt     # method-level log for testB
```

Each method-level log file contains:

```
... INFO  ... ========================= start executing test : MyTest#testA
... (test output) ...
... INFO  ... ========================= stop executing test : MyTest#testA
```

## TlsResourceExtension

`TlsResourceExtension` injects a `TlsResource` into JUnit lifecycle methods and test methods.
The extension creates a `TlsResource` lazily (only when injected) and ensures it is closed correctly.

## TlsResource

`TlsResource` is a test utility for creating and storing TLS-related artifacts. It can generate key/trust stores,
self-signed certificates, CRLs, and PEM/DER files under a temporary directory that is deleted on `close()`.
Use it directly with try-with-resources, or inject it via `TlsResourceExtension`.

### Common operations

- Create a self-signed key store or trust store
- Save private keys or certificates as PEM/DER files
- Create CRLs and convert them to data URLs

### Example: try-with-resources

```java
try (TlsResource tls = new TlsResource())
{
    Path keyStore = tls.createSelfSignedKeyStore("CN=localhost");
    Path trustStore = tls.createSelfSignedTrustStore("CN=localhost");
    Path privateKeyPem = tls.savePrivateKeyAsPem(
            TlsResourceBuilder.createRSAKeyPair().getPrivate());
}
```

### Lifecycle rules

- `@BeforeAll` receives one shared `TlsResource` per test class and it is closed after `@AfterAll`.
- `@BeforeEach` and `@Test` receive one `TlsResource` per test invocation; it is closed after the test completes.
- Within one invocation, `@BeforeEach` and the corresponding `@Test` use the same instance.

### JUnit configuration note

JUnit has a configuration parameter `junit.jupiter.extensions.store.close.autocloseable.enabled` that controls
whether `AutoCloseable` values stored in an `ExtensionContext.Store` are closed automatically. If this is disabled,
extensions that rely on that behavior can leak resources. `TlsResourceExtension` does not rely on this parameter and
closes resources explicitly, so it remains safe even if the parameter is set to `false`. Keep this in mind if you
implement other extensions that store `AutoCloseable` values in the JUnit store.

### Example: injection into a test method

```java
@ExtendWith({ TlsResourceExtension.class })
class MyTest
{
    @Test
    void testTls(final TlsResource tls) throws Exception
    {
        final Path ks = tls.createSelfSignedKeyStore("CN=localhost");
        // use ks ...
    }
}
```

### Example: injection into @BeforeEach

```java
@ExtendWith({ TlsResourceExtension.class })
class MyTest
{
    private Path trustStore;

    @BeforeEach
    void setUp(final TlsResource tls) throws Exception
    {
        trustStore = tls.createSelfSignedTrustStore("CN=localhost");
    }

    @Test
    void testUsesSameInstance(final TlsResource tls) throws Exception
    {
        // same tls instance as in @BeforeEach for this invocation
    }
}
```

### Example: injection into @BeforeAll

```java
@ExtendWith({ TlsResourceExtension.class })
class MyTest
{
    private static Path keyStore;

    @BeforeAll
    static void setUpClass(final TlsResource tls) throws Exception
    {
        keyStore = tls.createSelfSignedKeyStore("CN=localhost");
    }

    @Test
    void testUsesClassResource()
    {
        // keyStore is created once per class
    }
}
```
### Example: injection into both @BeforeAll and @BeforeEach

```java
@ExtendWith({ TlsResourceExtension.class })
class MyTest
{
    private static Path classLevelKeyStore;
    private Path testLevelKeyStore;

    @BeforeAll
    static void setUpClass(final TlsResource tls) throws Exception
    {
        classLevelKeyStore = tls.createSelfSignedKeyStore("CN=localhost");
    }

    @BeforeEach
    void setUp(final TlsResource tls) throws Exception
    {
        // tls instance is different from @BeforeAll invocation
        testLevelKeyStore = tls.createSelfSignedKeyStore("CN=localhost");
    }

    @Test
    void testUsesClassResource(final TlsResource tls)
    {
        // classLevelKeyStore is created once per class in @BeforeAll
        // testLevelKeyStore is created for this invocation in @BeforeEach
        // same tls instance as in @BeforeEach for this invocation
    }
}
```