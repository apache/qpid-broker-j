Module provides a static instrumentation agent, which transforms broker classes

* org.apache.qpid.server.model.ConfiguredObjectMethodAttributeOrStatistic
* org.apache.qpid.server.model.ConfiguredObjectMethodOperation
* org.apache.qpid.server.model.ConfiguredObjectTypeRegistry$AutomatedField

replacing reflection calls method.invoke() with static final MethodHandle.invokeExact().

To use instrumentation agent following JVM argument should be added to the broker start 
parameters:

```
-javaagent:$BROKER_DIR/lib/qpid-broker-instrumentation-${broker-version}.jar
```

List of classes to instrument can be supplied as a comma separated list:

```
-javaagent:$BROKER_DIR/lib/qpid-broker-instrumentation-${broker-version}.jar=ConfiguredObjectMethodAttributeOrStatistic
```

```
-javaagent:$BROKER_DIR/lib/qpid-broker-instrumentation-${broker-version}.jar=ConfiguredObjectMethodAttributeOrStatistic,ConfiguredObjectMethodOperation
```

```
-javaagent:$BROKER_DIR/lib/qpid-broker-instrumentation-${broker-version}.jar=ConfiguredObjectMethodAttributeOrStatistic,ConfiguredObjectMethodOperation,AutomatedField
```

When no arguments supplied, all classes will be instrumented.
