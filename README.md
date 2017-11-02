# Apache Qpid Broker-J

The Apache Qpid Broker-J is a powerful open-source message broker.

* Supports Advanced Message Queuing Protocol (AMQP) versions 0-8, 0-9, 0-91, 0-10 and 1.0
* 100% Java implementation
* Authentication options include for LDAP, Kerberos, O-AUTH2, TLS client-authentication and more
* Message storage options include Apache Derby, Oracle BDB JE, and Generic JDBC
* REST and AMQP 1.0 management API
* Web-management console
* Plug-able architecture

Below are some quick pointers you might find useful.

## Building the code

The project requires Maven 3. Some example commands follow.

Clean previous builds output and install all modules to local repository without running the tests:

    mvn clean install -DskipTests

Install all modules to the local repository after running all the tests:

    mvn clean install

## Running the tests

Maven profiles are used to run tests for the supported protocols and storage options.
Profile names follow the form *java-store.n-n*, where 
 *store* signifies the storage module and
 *n-n* the AMQP protocol version number.

For store, the options include:

* *bdb* - Oracle BDB JE
* *dby* - Apache Derby
* *mms* - an in-memory store principally used for testing.

If no profile is explicitly selected, *java-mms-1.0* is activated by default.

    mvn verify
    
To activate a BDB with AMQP 1.0 protocol use:

    mvn verify -P java-bdb.1-0

To see all the available profiles.

    mvn help:all-profiles

When activating AMQP 0-8..0-10 profiles, it is also necessary to pass the system property *-DenableAmqp0-x*

    mvn verify -P java-dby.0-9-1 -DenableAmqp0-x

Perform a subset of tests on the packaged release artifacts without installing:

    mvn verify -Dtest=TestNamePattern* -DfailIfNoTests=false

Execute the tests and produce code coverage report:

    mvn clean test jacoco:report

## Documentation

Documentation (in docbook format) is found beneath the *doc* module. The documentation is available in a published
form at:

http://qpid.apache.org/documentation.html


## Distribution assemblies

After packaging, the broker distribution assemblies can be found at:

    broker/target

To continue, see the Getting Started documentation in the docbook documentation mentioned above.
