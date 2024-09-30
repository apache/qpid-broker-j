#!/bin/bash
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

# This is the entry point for the docker images.
# This file is executed when "docker container create" or "docker run" is called.

set -e

if ! [ -f ./work/config.json ]; then
  if [ -d ./work-init ]; then
      for file in `ls ./work-init`; do echo copying file to work folder: $file; cp -r ./work-init/$file ./work || :; done
  fi
  sed -i "s/QPID_ADMIN_USER/${QPID_ADMIN_USER}/g" /qpid-broker-j/work/broker.acl
  if [ -d ./work-override ]; then
    for file in `ls ./work-override`; do echo copying file to work folder: $file; cp -r ./work-override/$file ./work || :; done
  fi
else
  echo "skipping broker instance creation; instance already exists"
fi

exec java -server $JAVA_GC $JAVA_MEM $JAVA_OPTS -DQPID_HOME=/qpid-broker-j -DQPID_WORK=/qpid-broker-j/work -cp "/qpid-broker-j/lib/*" org.apache.qpid.server.Main
