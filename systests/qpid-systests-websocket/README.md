<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
# WebSocket system tests

This module verifies existing WS and WSS transport behavior through a complete Broker-J instance.

From the repository root, install the module's dependencies, then run its system tests:

```shell
mvn -pl systests/qpid-systests-websocket -am install -DskipTests -DskipITs=true
mvn -pl systests/qpid-systests-websocket verify -DskipITs=false
```

The shared system-test parent binds these tests to the integration-test phase. Each invocation starts
an isolated broker using in-memory configuration and message stores, ephemeral loopback ports and
temporary TLS resources. Qpid JMS supplies independent client encoding and SASL negotiation.

| Class | Integration contract |
| --- | --- |
| `WebSocketMessagingTest` | Authentication, transacted delivery, rollback/redelivery and connection cleanup |
| `WebSocketInteroperabilityTest` | Messages and properties crossing TCP, WS and WSS with acknowledgements |
| `WebSocketExternalAuthenticationTest` | Client-certificate SASL EXTERNAL identity, messaging and missing-certificate rejection |
| `WebSocketTlsTest` | Rejection of an untrusted server certificate and subsequent trusted connection |

`WebSocketTestPeer` provides a bounded synchronous peer for controlled transport interactions.
Managed operations execute with the embedded broker's system subject.
