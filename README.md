# Apache Qpid Broker-J

---

|CI Process|Status|
|---|---|
|Travis CI Build|[![https://travis-ci.org/apache/qpid-broker-j.svg?branch=master](https://travis-ci.org/apache/qpid-broker-j.png?branch=master)](https://travis-ci.org/apache/qpid-broker-j?branch=master)|
|Apache Jenkins CI Build|[![Status](https://builds.apache.org/buildStatus/icon?job=Qpid-Broker-J-TestMatrix)](https://builds.apache.org/view/M-R/view/Qpid/job/Qpid-Broker-J-TestMatrix/)|

---

[Website](http://qpid.apache.org/) |
[Wiki](https://cwiki.apache.org/confluence/display/qpid) |
[Documentation](http://qpid.apache.org/documentation.html) |
[Developer Mailing List](mailto:dev@qpid.apache.org) |
[User Mailing List](mailto:users@qpid.apache.org) |
[Open Issues](https://issues.apache.org/jira/issues/?jql=project%20%3D%20QPID%20AND%20resolution%20%3D%20Unresolved%20AND%20component%20%3D%20Broker-J%20ORDER%20BY%20key%20DESC)

# Qpid Broker-J

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

Integration tests except for protocol tests are disabled by default.
In order to run all integration tests, they need to be enabled with a flag -DskipITs=false, for example

    mvn verify -DskipITs=false

Execute the tests and produce code coverage report:

    mvn clean test jacoco:report

## Distribution assemblies

To produce broker assemblies, use:

    mvn clean package -DskipTests

The broker distribution assemblies will then be found beneath:

   apache-qpid-broker-j/target

## Running the Broker

For full details, see the Getting Started documentation in the docbook documentation mentioned below.  For convenience brief instructions are repeated here.


### Start the Broker

#### From an assembly on the command line

Expand the assembly produced by the Maven package lifecycle stage, and the execute the qpid-server script or batch file.

On UNIX:

    tar xvfz apache-qpid-broker-j/target/apache-qpid-broker-j-x.x.x-SNAPSHOT-bin.tar.gz
    ./java-broker/x.x.x/qpid-server

On Windows:

    Expand zip apache-qpid-broker-j/target/apache-qpid-broker-j-x.x.x-SNAPSHOT-bin.zip
    .\java-broker\x.x.x\qpid-server.bat

#### From an IDE

These instructions assume Intellij.

1. Within the IDE import the top level pom.xml file as a project.
2. Create a new Application run configuration called "Qpid Broker"
 * Classname org.apache.qpid.server.Main
 * Classpath of module: qpid-broker
 * Save the new run configuration.
3. Go into the module settings of qpid-broker and add an additional classpath entry pointing to the dojo-x.x.x-distribution.zip. It is easiest to point to a location in the local maven repo e.g. ~/.m2/repository/org/dojotoolkit/dojo/x.x.x/dojo-x.x.x-distribution.zip.  (This manual step is required to workaround https://issues.apache.org/jira/browse/MNG-5567).

### Connecting to the Broker

By default, the Broker listens on port 5672 for AMQP and 8080 for http management.  The default
username 'guest' and password 'guest'.

To get to the management console, point a browser to http://localhost:8080

## Documentation

Documentation (in docbook format) is found beneath the *doc* module.

