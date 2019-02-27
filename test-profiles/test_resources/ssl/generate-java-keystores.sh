#!/usr/bin/env bash
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

echo "Remove existing keystore for Apache Qpid Broker-J "
rm java_broker_keystore.jks
echo "Re-create keystore for Apache Qpid Broker-J  by importing RootCA certificate"
keytool -importcert -v -keystore java_broker_keystore.jks -keysize 2048 -storepass password -alias RootCA -file CA_db/rootca.crt -storetype pkcs12 -noprompt
echo "Generate certificate key 'java-broker'"
keytool -genkey -alias java-broker -keyalg RSA -keysize 2048 -sigalg SHA512withRSA -validity 720 -keystore java_broker_keystore.jks -storepass password -dname "CN=localhost, OU=Unknown, O=Unknown, L=Unknown, ST=Unknown, C=Unknown"  -storetype pkcs12
echo "Export certificate signing request"
keytool -certreq -alias java-broker -sigalg SHA512withRSA -keystore java_broker_keystore.jks -storepass password -v -file java_broker.req  -storetype pkcs12
echo "Sign certificate by entering:"
echo "  n for 'Is this a CA certificate [y/N]?'"
echo "  [Enter] for 'Enter the path length constraint, enter to skip [<0 for unlimited path]: >'"
echo "  n for 'Is this a critical extension [y/N]?'"
echo "  password which was specified on creation root CA database."
certutil -C -d CA_db -c "MyRootCA" -a -i java_broker.req -o java_broker.crt -2 -6 --extKeyUsage serverAuth -v 60 -g 4096
echo "Import signed certificate"
keytool -importcert -v -alias java-broker -keystore java_broker_keystore.jks -storepass password -file java_broker.crt  -storetype pkcs12 -noprompt
echo "List keystore entries"
keytool --list --keystore java_broker_keystore.jks -storepass password  -storetype pkcs12

read -p "Press [Enter] key to continue..."
echo "Remove existing client keystore"
rm java_client_keystore.jks
echo "Re-create client keystore by importing RootCA certificate"
keytool -importcert -v -keystore java_client_keystore.jks -storepass password -alias RootCA -file CA_db/rootca.crt  -storetype pkcs12 -noprompt

echo "Generate key for certificate 'app2'"
keytool -genkey -alias app2 -keyalg RSA -keysize 2048 -sigalg SHA512withRSA -validity 720 -keystore java_client_keystore.jks -storepass password  -dname "CN=app2@acme.org, OU=art, O=acme, L=Toronto, ST=ON, C=CA"  -storetype pkcs12
echo "Export certificate signing request for 'app2'"
keytool -certreq -alias app2 -sigalg SHA512withRSA -keystore java_client_keystore.jks -storepass password -v -file app2.req  -storetype pkcs12
echo "Sign certificate 'app2' by entering:"
echo "  n for 'Is this a CA certificate [y/N]?'"
echo "  '-1' for 'Enter the path length constraint, enter to skip [<0 for unlimited path]: >'"
echo "  n for 'Is this a critical extension [y/N]?'"
echo "  password which was specified on creation root CA database."
certutil -C -d CA_db -c "MyRootCA" -a -i app2.req -o app2.crt -2 -6  --extKeyUsage clientAuth -v 60 -Z SHA512
echo "Import signed certificate 'app2'"
keytool -importcert -v -alias app2 -keystore java_client_keystore.jks -storepass password -file app2.crt  -storetype pkcs12 -noprompt

echo "Generate key for certificate 'app1'"
keytool -genkey -alias app1 -keyalg RSA -keysize 2048 -sigalg SHA512withRSA -validity 720 -keystore java_client_keystore.jks -storepass password  -dname "CN=app1@acme.org, OU=art, O=acme, L=Toronto, ST=ON, C=CA"  -storetype pkcs12
echo "Export certificate signing request for 'app1'"
keytool -certreq -alias app1 -sigalg SHA512withRSA -keystore java_client_keystore.jks -storepass password -v -file app1.req
echo "Sign certificate 'app1' by entering:"
echo "  n for 'Is this a CA certificate [y/N]?'"
echo "  '-1' for 'Enter the path length constraint, enter to skip [<0 for unlimited path]: >'"
echo "  n for 'Is this a critical extension [y/N]?'"
echo "  password which was specified on creation of root CA database."
certutil -C -d CA_db -c "MyRootCA" -a -i app1.req -o app1.crt -2 -6  --extKeyUsage clientAuth -v 60 -Z SHA512
echo "Import signed certificate 'app1'"
keytool -importcert -v -alias app1 -keystore java_client_keystore.jks -storepass password -file app1.crt  -storetype pkcs12 -noprompt
echo "List entries in client keystore"
keytool --list --keystore java_client_keystore.jks  -storepass password

read -p "Press [Enter] key to continue..."
echo "Remove existing client truststore"
rm java_client_truststore.jks 
echo "Re-create client truststore by importing RootCA certificate"
keytool -importcert -v -keystore java_client_truststore.jks -storepass password -alias RootCA -file CA_db/rootca.crt  -storetype pkcs12 -noprompt
echo "List entries in client trusttore"
keytool --list --keystore java_client_truststore.jks  -storepass password  -storetype pkcs12

read -p "Press [Enter] key to continue..."
echo "Remove existing broker truststore"
rm java_broker_truststore.jks
echo "Re-create broker truststore by importing RootCA certificate"
keytool -importcert -v -keystore java_broker_truststore.jks -storepass password -alias RootCA -file CA_db/rootca.crt  -storetype pkcs12 -noprompt
echo "List entries in broker truststore"
keytool --list --keystore java_broker_truststore.jks  -storepass password  -storetype pkcs12

read -p "Press [Enter] key to continue..."
echo "Remove existing broker peerstore"
rm java_broker_peerstore.jks 
echo "Re-create broker peerstore by importing app1 certificate"
keytool -importcert -v -keystore java_broker_peerstore.jks -storepass password -alias app1 -file app1.crt  -storetype pkcs12 -noprompt
echo "List entries in broker peerstore"
keytool --list --keystore java_broker_peerstore.jks  -storepass password  -storetype pkcs12

cp java_broker_keystore.jks ../../../broker-core/src/test/resources/ssl/test_keystore.jks
keytool -importcert -v -alias app1 -keystore ../../../broker-core/src/test/resources/ssl/test_keystore.jks -storepass password -file app1.crt  -storetype pkcs12 -noprompt
keytool -importcert -v -alias app2 -keystore ../../../broker-core/src/test/resources/ssl/test_keystore.jks -storepass password -file app2.crt  -storetype pkcs12 -noprompt

cp java_broker_keystore.jks ../../../broker-core/src/test/resources/ssl/test_pk_only_keystore.pkcs12
keytool -delete -v -alias rootca  -keystore ../../../broker-core/src/test/resources/ssl/test_pk_only_keystore.pkcs12 -storepass password

cp java_broker_keystore.jks ../../../broker-core/src/test/resources/ssl/test_cert_only_keystore.pkcs12
keytool -delete -v -alias java-broker  -keystore ../../../broker-core/src/test/resources/ssl/test_cert_only_keystore.pkcs12 -storepass password

cp java_broker_keystore.jks ../../../broker-core/src/test/resources/ssl/test_symmetric_key_keystore.pkcs12
keytool -genseckey -alias testalias -keyalg AES -keysize 256 -storetype pkcs12 -keystore ../../../broker-core/src/test/resources/ssl/test_symmetric_key_keystore.pkcs12 -storepass password

cp java_broker.req ../../../broker-core/src/test/resources/ssl/java_broker.req
cp java_broker.crt ../../../broker-core/src/test/resources/ssl/java_broker.crt

cp expired.crt ../../../broker-core/src/test/resources/ssl/expired.crt
cp java_client_expired_keystore.jks ../../../broker-core/src/test/resources/ssl/java_client_expired_keystore.pkcs12
cp java_broker_expired_truststore.jks ../../../broker-core/src/test/resources/ssl/java_broker_expired_truststore.pkcs12

cp java_broker_peerstore.jks ../../../broker-core/src/test/resources/ssl/java_broker_peerstore.pkcs12
cp java_broker_truststore.jks  ../../../broker-core/src/test/resources/ssl/java_broker_truststore.pkcs12
cp java_broker_keystore.jks  ../../../broker-core/src/test/resources/ssl/java_broker_keystore.pkcs12
cp java_broker_keystore.jks  ../../../systests/qpid-systests-http-management/src/main/resources/java_broker_keystore.jks
cp java_client_keystore.jks  ../../../broker-core/src/test/resources/ssl/java_client_keystore.pkcs12
cp java_client_truststore.jks  ../../../broker-core/src/test/resources/ssl/java_client_truststore.pkcs12

rm java_client_untrusted_keystore.jks
keytool -genkey -keystore java_client_untrusted_keystore.jks -keyalg RSA -keysize 2048 -sigalg SHA512withRSA -alias untrusted_client -storepass password  -storetype pkcs12 -dname "CN=untrusted_client"
cp java_client_untrusted_keystore.jks  ../../../broker-core/src/test/resources/ssl/java_client_untrusted_keystore.pkcs12


