#
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#


env.ts_unix.testsuite=com.sun.ts.lib.harness.TS
env.ts_unix.command.testExecute=com.sun.ts.lib.harness.ExecTSTestCmd \
        CLASSPATH=${TS_HOME}/classes${path.separator}${LOCAL_CLASSES} \
        ${JAVA_HOME}/bin/java \
		-Ddeliverable.class=${deliverable.class} \
		-Djava.naming.factory.initial=${java.naming.factory.initial} \
		-Djava.naming.provider.url=${java.naming.provider.url} \
	    ${JAVA_OPTIONS} $testExecuteClass $testExecuteArgs

ts_home=${TS_HOME}
harness.log.port=2001
harness.log.traceflag=true
harness.log.delayseconds=0
porting.ts.jmsObjects.class.1=${tck.jmsobjects.class}

jms_timeout=5000
user=${tck.management-username}
password=${tck.management-password}

# properties required for JMS 2.0 TCK
platform.mode=standalone
harness.socket.retry.count=10
