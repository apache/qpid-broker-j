#!/bin/sh
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

readonly MY_PATH="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
readonly PASSWORD=password
readonly ROOT_CA=MyRootCA
readonly INTERMEDIATE_CA=intermediate_ca
readonly OPENSSL_DIR="$MY_PATH/openssl"
readonly OPENSSL_CONF="$OPENSSL_DIR/openssl.conf"
readonly CERTIFICATES_DIR="$MY_PATH/certificates"
readonly VALID_DAYS=1461

readonly CLIENT_KEYSTORE="$CERTIFICATES_DIR/client_keystore.jks"
readonly CLIENT_TRUSTSTORE="$CERTIFICATES_DIR/client_truststore.jks"
readonly CLIENT_EXPIRED_KEYSTORE="$CERTIFICATES_DIR/client_expired_keystore.jks"
readonly CLIENT_EXPIRED_CRT="$CERTIFICATES_DIR/client_expired.crt"
readonly CLIENT_UNTRUSTED_KEYSTORE="$CERTIFICATES_DIR/client_untrusted_keystore.jks"

readonly BROKER_KEYSTORE="$CERTIFICATES_DIR/broker_keystore.jks"
readonly BROKER_TRUSTSTORE="$CERTIFICATES_DIR/broker_truststore.jks"
readonly BROKER_PEERSTORE="$CERTIFICATES_DIR/broker_peerstore.jks"
readonly BROKER_EXPIRED_TRUSTSTORE="$CERTIFICATES_DIR/broker_expired_truststore.jks"
readonly BROKER_CRT="$CERTIFICATES_DIR/broker.crt"
readonly BROKER_CSR="$CERTIFICATES_DIR/broker.csr"
readonly BROKER_ALIAS="broker"

readonly TEST_KEYSTORE="$CERTIFICATES_DIR/test_keystore.jks"
readonly TEST_PK_ONLY_KEYSTORE="$CERTIFICATES_DIR/test_pk_only_keystore.jks"
readonly TEST_CERT_ONLY_KEYSTORE="$CERTIFICATES_DIR/test_cert_only_keystore.jks"
readonly TEST_SYMMETRIC_KEY_KEYSTORE="$CERTIFICATES_DIR/test_symmetric_key_keystore.jks"
readonly TEST_EMPTY_KEYSTORE="$CERTIFICATES_DIR/test_empty_keystore.jks"

# set to true for debug
readonly DEBUG=false

generate_selfsigned_ca()
{
    echo "Generating selfsigned CA certificate"
    openssl req -x509 -newkey rsa:2048 -keyout "$CERTIFICATES_DIR/$ROOT_CA.key" -out "$CERTIFICATES_DIR/$ROOT_CA.crt" -days 1461 -subj '/C=CA/ST=Ontario/O=ACME/CN=MyRootCA' -passout pass:$PASSWORD -sha512 && \
    keytool -import -alias rootca -file "$CERTIFICATES_DIR/$ROOT_CA.crt" -storepass "$PASSWORD" -noprompt -deststoretype PKCS12 -keystore "$CLIENT_KEYSTORE" && \
    keytool -import -alias rootca -file "$CERTIFICATES_DIR/$ROOT_CA.crt" -storepass "$PASSWORD" -noprompt -deststoretype PKCS12 -keystore "$CLIENT_TRUSTSTORE" && \
    keytool -import -alias rootca -file "$CERTIFICATES_DIR/$ROOT_CA.crt" -storepass "$PASSWORD" -noprompt -deststoretype PKCS12 -keystore "$BROKER_KEYSTORE" && \
    keytool -import -alias rootca -file "$CERTIFICATES_DIR/$ROOT_CA.crt" -storepass "$PASSWORD" -noprompt -deststoretype PKCS12 -keystore "$BROKER_TRUSTSTORE"
    local _rc=$?
    if [ $_rc -eq 0 ]; then
        echo "Selfsigned CA certificate successfully generated"
    else
        echo "Failed to generate selfsigned CA certificate" >&2
    fi
    return $_rc
}

prepare_openssl_environment()
{
    echo "Preparing openssl environment"
    rm -rf "$CERTIFICATES_DIR" && \
    mkdir "$CERTIFICATES_DIR" && \
    rm -rf "$OPENSSL_DIR" && \
    mkdir "$OPENSSL_DIR" && \
    cp "$MY_PATH/openssl.conf" "$OPENSSL_DIR" && \
    sed -i "s|^dir             = .|dir             = $OPENSSL_DIR|" "$OPENSSL_CONF" && \
    echo 1234 > "$OPENSSL_DIR"/serial && \
    echo 1234 > "$OPENSSL_DIR"/crlnumber && \
    touch "$OPENSSL_DIR"/index.txt && \
    echo "unique_subject = no" > "$OPENSSL_DIR"/index.txt.attr && \
    mkdir "$OPENSSL_DIR"/newcerts
    local _rc=$?
    if [ $_rc -eq 0 ]; then
        echo "Openssl environment successfully prepared"
    else
        echo "Failed to prepare openssl environment" >&2
    fi
    return $_rc
}

# $1 - alias
generate_signed_certificate()
{
    local _alias=$1
    local _subject="/C=CA/ST=ON/L=Toronto/O=acme/OU=art/CN=$_alias@acme.org"
    echo "Generating CA signed certificate '$_alias'"
    openssl req -x509 -newkey rsa:2048 -keyout "$CERTIFICATES_DIR/$_alias.self.key" -out "$CERTIFICATES_DIR/$_alias.self.crt" -subj "$_subject" -sha512 -passout pass:$PASSWORD && \
    openssl req -config "$OPENSSL_CONF" -new -key "$CERTIFICATES_DIR/$_alias.self.key" -out "$CERTIFICATES_DIR/$_alias.csr" -sha512 -subj "$_subject" -passin pass:$PASSWORD && \
    openssl ca -config "$OPENSSL_CONF" -md sha512 -extensions v3_req -batch -passin pass:$PASSWORD -out "$CERTIFICATES_DIR/$_alias.crt" -keyfile "$CERTIFICATES_DIR/$ROOT_CA.key" -cert "$CERTIFICATES_DIR/$ROOT_CA.crt" -days $VALID_DAYS -infiles "$CERTIFICATES_DIR/$_alias.csr" && \
    openssl pkcs12 -export -chain -CAfile "$CERTIFICATES_DIR/$ROOT_CA.crt" -in "$CERTIFICATES_DIR/$_alias.crt" -inkey "$CERTIFICATES_DIR/$_alias.self.key" -out "$CERTIFICATES_DIR/$_alias.jks" -name $_alias -passin pass:"$PASSWORD" -passout pass:"$PASSWORD" && \
    keytool -importkeystore -srckeystore "$CERTIFICATES_DIR/$_alias.jks" -srcstoretype PKCS12 -storepass "$PASSWORD" -srcstorepass "$PASSWORD" -alias $_alias -deststoretype PKCS12 -destkeystore "$CLIENT_KEYSTORE"
    local _rc=$?
    if [ $_rc -eq 0 ]; then
        echo "CA signed certificate '$_alias' successfully generated"
    else
        echo "Failed to generate CA signed certificate '$_alias'" >&2
    fi
    return $_rc
}

# $1 - certificate alias
generate_signed_certificate_with_intermediate_signed_certificate()
{
    local _alias=$1
    local _intermediate_ca_subject="/C=CA/ST=ON/L=Toronto/O=acme/OU=art/CN=$INTERMEDIATE_CA@acme.org"
    local _subject="/C=CA/ST=ON/L=Toronto/O=acme/OU=art/CN=$_alias@acme.org"
    echo "Generating CA signed certificate '$_alias' with intermediate CA certificate '$INTERMEDIATE_CA'"
    openssl req -x509 -newkey rsa:2048 -keyout "$CERTIFICATES_DIR/$INTERMEDIATE_CA.self.key" -out "$CERTIFICATES_DIR/$INTERMEDIATE_CA.self.crt" -subj "$_intermediate_ca_subject" -sha512 -passout pass:$PASSWORD && \
    openssl req -config "$OPENSSL_CONF" -verbose -new -key "$CERTIFICATES_DIR/$INTERMEDIATE_CA.self.key" -out "$CERTIFICATES_DIR/$INTERMEDIATE_CA.csr" -sha512 -subj "$_intermediate_ca_subject" -passin pass:$PASSWORD && \
    openssl ca -config "$OPENSSL_CONF" -md sha512 -extensions v3_ca -batch -passin pass:$PASSWORD -out "$CERTIFICATES_DIR/$INTERMEDIATE_CA.crt" -keyfile "$CERTIFICATES_DIR/$ROOT_CA.key" -cert "$CERTIFICATES_DIR/$ROOT_CA.crt" -days $VALID_DAYS -infiles "$CERTIFICATES_DIR/$INTERMEDIATE_CA.csr" && \
    openssl pkcs12 -export -chain -CAfile "$CERTIFICATES_DIR/$ROOT_CA.crt" -in "$CERTIFICATES_DIR/$INTERMEDIATE_CA.crt" -inkey "$CERTIFICATES_DIR/$INTERMEDIATE_CA.self.key" -out "$CERTIFICATES_DIR/$INTERMEDIATE_CA.jks" -name $INTERMEDIATE_CA -passin pass:"$PASSWORD" -passout pass:"$PASSWORD"
    echo "Generating CA signed certificate for '$_alias'" && \
    openssl req -x509 -newkey rsa:2048 -keyout "$CERTIFICATES_DIR/$_alias.self.key" -out "$CERTIFICATES_DIR/$_alias.self.crt" -subj "$_subject" -sha512 -passout pass:$PASSWORD && \
    openssl req -config "$OPENSSL_CONF" -verbose -new -key "$CERTIFICATES_DIR/$_alias.self.key" -out "$CERTIFICATES_DIR/$_alias.csr" -sha512 -subj "$_subject" -passin pass:$PASSWORD && \
    openssl ca -config "$OPENSSL_CONF" -md sha512 -extensions v3_req -batch -passin pass:$PASSWORD -out "$CERTIFICATES_DIR/$_alias.crt" -keyfile "$CERTIFICATES_DIR/$INTERMEDIATE_CA.self.key" -cert "$CERTIFICATES_DIR/$INTERMEDIATE_CA.crt" -days $VALID_DAYS -infiles "$CERTIFICATES_DIR/$_alias.csr" && \
    cat "$CERTIFICATES_DIR/$INTERMEDIATE_CA.crt" "$CERTIFICATES_DIR/$ROOT_CA.crt" > "$CERTIFICATES_DIR/chain_with_intermediate.crt"
    openssl pkcs12 -export -chain -CAfile "$CERTIFICATES_DIR/chain_with_intermediate.crt" -in "$CERTIFICATES_DIR/$_alias.crt" -inkey "$CERTIFICATES_DIR/$_alias.self.key" -out "$CERTIFICATES_DIR/$_alias.jks" -name $_alias -passin pass:"$PASSWORD" -passout pass:"$PASSWORD" && \
    keytool -importkeystore -srckeystore "$CERTIFICATES_DIR/$_alias.jks" -srcstoretype PKCS12 -storepass "$PASSWORD" -srcstorepass "$PASSWORD" -alias $_alias -deststoretype PKCS12 -destkeystore "$CLIENT_KEYSTORE"
    local _rc=$?
    if [ $_rc -eq 0 ]; then
        echo "CA signed certificate '$_alias' with intermediate CA certificate '$INTERMEDIATE_CA' successfully generated"
    else
        echo "Failed to generate CA signed certificate '$_alias' with intermediate CA certificate '$INTERMEDIATE_CA'" >&2
    fi
    return $_rc
}

generate_expired_certificate()
{
    local _alias=user1
    echo "Generating expired certificate '$_alias'"
    keytool -genkeypair -alias $_alias -dname CN=USER1 -startdate "2010/01/01 12:00:00" -validity $VALID_DAYS -keysize 2048 -keyalg RSA -sigalg SHA512withRSA -keypass "$PASSWORD" -storepass "$PASSWORD" -deststoretype PKCS12 -keystore "$CLIENT_EXPIRED_KEYSTORE" && \
    keytool -exportcert -keystore "$CLIENT_EXPIRED_KEYSTORE" -storepass "$PASSWORD" -alias $_alias -rfc -file "$CLIENT_EXPIRED_CRT" && \
    keytool -import -alias $_alias -file "$CLIENT_EXPIRED_CRT" -storepass "$PASSWORD" -noprompt -deststoretype PKCS12 -sigalg SHA512withRSA -keystore "$BROKER_EXPIRED_TRUSTSTORE"
    local _rc=$?
    if [ $_rc -eq 0 ]; then
        echo "Expired certificate '$_alias' successfully generated"
    else
        echo "Failed to generate expired certificate '$_alias'" >&2
    fi
    return $_rc
}

generate_signed_broker_certificate()
{
    local _subject="/C=CA/ST=Unknown/L=Unknown/O=Unknown/OU=Unknown/CN=localhost"
    echo "Generating CA signed certificate '$BROKER_ALIAS'"
    openssl req -x509 -newkey rsa:2048 -keyout "$CERTIFICATES_DIR/$BROKER_ALIAS.self.key" -out "$CERTIFICATES_DIR/$BROKER_ALIAS.self.crt" -subj "$_subject" -passout pass:$PASSWORD && \
    openssl req -config "$OPENSSL_CONF" -verbose -new -key "$CERTIFICATES_DIR/$BROKER_ALIAS.self.key" -out "$BROKER_CSR" -sha512 -subj "$_subject" -passin pass:$PASSWORD && \
    openssl ca -config "$OPENSSL_CONF" -md sha512 -extensions v3_req -batch -passin pass:$PASSWORD -out "$BROKER_CRT" -keyfile "$CERTIFICATES_DIR/$ROOT_CA.key" -cert "$CERTIFICATES_DIR/$ROOT_CA.crt" -days $VALID_DAYS -infiles "$BROKER_CSR" && \
    openssl pkcs12 -export -chain -CAfile "$CERTIFICATES_DIR/$ROOT_CA.crt" -in "$BROKER_CRT" -inkey "$CERTIFICATES_DIR/$BROKER_ALIAS.self.key" -out "$CERTIFICATES_DIR/$BROKER_ALIAS.jks" -name $BROKER_ALIAS -passin pass:"$PASSWORD" -passout pass:"$PASSWORD" && \
    keytool -importkeystore -srckeystore "$CERTIFICATES_DIR/$BROKER_ALIAS.jks" -srcstoretype PKCS12 -storepass "$PASSWORD" -srcstorepass "$PASSWORD" -alias $BROKER_ALIAS -deststoretype PKCS12 -destkeystore "$BROKER_KEYSTORE"
    local _rc=$?
    if [ $_rc -eq 0 ]; then
        echo "CA signed certificate '$BROKER_ALIAS' successfully generated"
    else
        echo "Failed to generate CA signed certificate '$BROKER_ALIAS'" >&2
    fi
    return $_rc
}

# $1 - certificate alias
# $2 - keystore where certificate will be imported
import_to_keystore()
{
    local _alias=$1
    local _keystore="$2"

    echo "Importing certificate '$_alias' to keystore '$_keystore'"
    keytool -import -alias $_alias -file "$CERTIFICATES_DIR/$_alias.crt" -storepass "$PASSWORD" -noprompt -deststoretype PKCS12 -sigalg SHA512withRSA -keystore "$_keystore"
    local _rc=$?
    if [ $_rc -eq 0 ]; then
        echo "Certificate '$_alias' successfully imported to keystore '$_keystore'"
    else
        echo "Failed to import certificate '$_alias' to keystore '$_keystore'" >&2
    fi
    return $_rc
}

generate_untrusted_client_certificate()
{
    local _alias=untrusted_client

    echo "Generating untrusted certificate '$_alias'"
    keytool -genkeypair -alias $_alias -dname CN=$_alias -validity $VALID_DAYS -keysize 2048 -keyalg RSA -sigalg SHA512withRSA -keypass "$PASSWORD" -storepass "$PASSWORD" -deststoretype PKCS12 -keystore "$CLIENT_UNTRUSTED_KEYSTORE"
    local _rc=$?
    if [ $_rc -eq 0 ]; then
        echo "Untrusted certificate '$_alias' successfully generated"
    else
        echo "Failed to generate untrusted certificate '$_alias'" >&2
    fi
    return $_rc
}

add_certificate_crl_distribution_point()
{
    echo "Add CRL distribution points to openssl configuration"
    sed -i "/\[ v3_req \]/a crlDistributionPoints=URI:file://$CERTIFICATES_DIR/$ROOT_CA.crl" "$OPENSSL_CONF" && \
    sed -i "/\[ v3_ca \]/a crlDistributionPoints=URI:file://$CERTIFICATES_DIR/$ROOT_CA.crl" "$OPENSSL_CONF"
    local _rc=$?
    if [ $_rc -eq 0 ]; then
        echo "CRL distribution points successfully addded"
    else
        echo "Failed to add CRL distribution points" >&2
    fi
    return $_rc
}

set_certificate_crl_distribution_point_to_intermediate_ca()
{
    echo "Setting CRL distribution point for intermediate CA certificate '$INTERMEDIATE_CA'"
    sed -i -z "s|crlDistributionPoints=URI:file://$CERTIFICATES_DIR/$ROOT_CA.crl|crlDistributionPoints=URI:file://$CERTIFICATES_DIR/$INTERMEDIATE_CA.crl|" "$OPENSSL_CONF"
    local _rc=$?
    if [ $_rc -eq 0 ]; then
        echo "CRL distribution point for intermediate CA certificate '$INTERMEDIATE_CA' successfully set"
    else
        echo "Failed to set CRL distribution point for intermediate CA certificate '$INTERMEDIATE_CA'" >&2
    fi
    return $_rc
}

set_certificate_crl_distribution_point_to_empty_crl()
{
    # change only first occurence (v3_req), commented line changes all occurences
    # sed -i "s|crlDistributionPoints=URI:file://$CERTIFICATES_DIR/$ROOT_CA.crl|crlDistributionPoints=URI:file://$CERTIFICATES_DIR/$ROOT_CA.empty.crl|" "$OPENSSL_CONF"
    echo "Setting CRL distribution point to empty CRL"
    sed -i -z "s|crlDistributionPoints=URI:file://$CERTIFICATES_DIR/$INTERMEDIATE_CA.crl|crlDistributionPoints=URI:file://$CERTIFICATES_DIR/$ROOT_CA.empty.crl|" "$OPENSSL_CONF"
    local _rc=$?
    if [ $_rc -eq 0 ]; then
        echo "CRL distribution point to empty CRL successfully set"
    else
        echo "Failed to set CRL distribution to empty CRL" >&2
    fi
    return $_rc
}

set_certificate_crl_distribution_point_to_invalid_crl_path()
{
    echo "Setting CRL distribution point to invalid CRL path"
    sed -i "s|crlDistributionPoints=URI:file://$CERTIFICATES_DIR/$ROOT_CA.empty.crl|crlDistributionPoints=URI:file:///not/a/crl|" "$OPENSSL_CONF"
    local _rc=$?
    if [ $_rc -eq 0 ]; then
        echo "CRL distribution point to invalid CRL path successfully set"
    else
        echo "Failed to set CRL distribution to invalid CRL path" >&2
    fi
    return $_rc
}

generate_intermediate_crl()
{
    echo "Generating intermediate CA certificate '$INTERMEDIATE_CA' CRL"
    openssl ca -config "$OPENSSL_CONF" -passin pass:$PASSWORD -gencrl -keyfile "$CERTIFICATES_DIR/$INTERMEDIATE_CA.self.key" -cert "$CERTIFICATES_DIR/$INTERMEDIATE_CA.crt" -out "$CERTIFICATES_DIR/$INTERMEDIATE_CA.crl.pem" && \
    openssl crl -inform PEM -in "$CERTIFICATES_DIR/$INTERMEDIATE_CA.crl.pem" -outform DER -out "$CERTIFICATES_DIR/$INTERMEDIATE_CA.crl"
    local _rc=$?
    if [ $_rc -eq 0 ]; then
        echo "Intermediate CA certificate '$INTERMEDIATE_CA' CRL successfully generated"
    else
        echo "Failed to generate intermediate CA certificate '$INTERMEDIATE_CA' CRL" >&2
    fi
    return $_rc
}


# $1 - part of CRL file name
generate_crl()
{
    local _crl_name_part=$1
    local _crl_path_prefix
    if [ -n "$_crl_name_part" ]; then
        _crl_path_prefix="$CERTIFICATES_DIR/$ROOT_CA.$_crl_name_part"
    else
        _crl_path_prefix="$CERTIFICATES_DIR/$ROOT_CA"
    fi

    echo "Generating certificate '$ROOT_CA' CRL to '$_crl_path_prefix'"
    openssl ca -config "$OPENSSL_CONF" -passin pass:$PASSWORD -gencrl -keyfile "$CERTIFICATES_DIR/$ROOT_CA.key" -cert "$CERTIFICATES_DIR/$ROOT_CA.crt" -out "$_crl_path_prefix.crl.pem" && \
    openssl crl -inform PEM -in "$_crl_path_prefix.crl.pem" -outform DER -out "$_crl_path_prefix.crl"
    local _rc=$?
    if [ $_rc -eq 0 ]; then
        echo "Certificate '$ROOT_CA' CRL successfully generated to '$_crl_path_prefix'"
    else
        echo "Failed to generate certificate '$ROOT_CA' CRL to '$_crl_path_prefix'" >&2
    fi
    return $_rc
}

revoke_certificate()
{
    local _alias=$1

    echo "Revoking certificate '$_alias'"
    openssl ca -config "$OPENSSL_CONF" -passin pass:$PASSWORD -revoke "$CERTIFICATES_DIR/$_alias.crt" -keyfile "$CERTIFICATES_DIR/$ROOT_CA.key" -cert "$CERTIFICATES_DIR/$ROOT_CA.crt"
    local _rc=$?
    if [ $_rc -eq 0 ]; then
        echo "Certificate '$_alias' successfully revoked"
    else
        echo "Failed to revoke certificate '$_alias'" >&2
    fi
    return $_rc
}

prepare_test_keystores()
{
    echo "Preparing test keystores"
    cp "$BROKER_KEYSTORE" "$TEST_KEYSTORE" && \
    import_to_keystore "app1" "$TEST_KEYSTORE" && \
    import_to_keystore "app2" "$TEST_KEYSTORE" && \
    cp "$BROKER_KEYSTORE" "$TEST_PK_ONLY_KEYSTORE" && \
    keytool -delete -v -alias rootca -storepass password -keystore "$TEST_PK_ONLY_KEYSTORE" && \
    cp "$BROKER_KEYSTORE" "$TEST_CERT_ONLY_KEYSTORE" && \
    keytool -delete -v -alias $BROKER_ALIAS -storepass password -keystore "$TEST_CERT_ONLY_KEYSTORE" && \
    cp "$BROKER_KEYSTORE" "$TEST_SYMMETRIC_KEY_KEYSTORE" && \
    keytool -genseckey -alias testalias -keyalg AES -keysize 256 -storetype PKCS12 -storepass "$PASSWORD" -keystore "$TEST_SYMMETRIC_KEY_KEYSTORE" && \
    cp "$TEST_PK_ONLY_KEYSTORE" "$TEST_EMPTY_KEYSTORE"
    keytool -delete -v -alias $BROKER_ALIAS -storepass password -keystore "$TEST_EMPTY_KEYSTORE" && \
    local _rc=$?
    if [ $_rc -eq 0 ]; then
        echo "Test keystores prepared"
    else
        echo "Failed to prepare keystores" >&2
    fi
    return $_rc
}

main()
{
    prepare_openssl_environment && \
    generate_selfsigned_ca && \
    generate_signed_certificate "app1" && \
    generate_signed_certificate "app2" && \
    generate_expired_certificate && \
    generate_signed_broker_certificate && \
    import_to_keystore "app1" "$BROKER_PEERSTORE" && \
    generate_untrusted_client_certificate && \
    add_certificate_crl_distribution_point && \
    generate_signed_certificate "allowed_by_ca" && \
    generate_signed_certificate "revoked_by_ca" && \
    set_certificate_crl_distribution_point_to_intermediate_ca && \
    generate_signed_certificate_with_intermediate_signed_certificate "allowed_by_ca_with_intermediate" && \
    generate_intermediate_crl && \
    set_certificate_crl_distribution_point_to_empty_crl && \
    generate_signed_certificate "revoked_by_ca_empty_crl" && \
    set_certificate_crl_distribution_point_to_invalid_crl_path && \
    generate_signed_certificate "revoked_by_ca_invalid_crl_path" && \
    generate_crl "empty" && \
    revoke_certificate "$INTERMEDIATE_CA" && \
    revoke_certificate "revoked_by_ca" && \
    revoke_certificate "revoked_by_ca_empty_crl" && \
    revoke_certificate "revoked_by_ca_invalid_crl_path" && \
    generate_crl && \
    prepare_test_keystores
}

if [ "$DEBUG" == true ]; then
    main
else
    main 2>/dev/null 1>&2
fi
