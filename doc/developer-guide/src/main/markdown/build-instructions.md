# Build Instructions

[Quick Start Guide](quick-start.md) only gives a minimum set of instructions to start quickly building and developing
Qpid Broker-J code. The materials provided here intend to give a deeper understanding of various options for building,
and testing Qpid Broker-J.

<!-- toc -->

- [Prerequisites](#prerequisites)
  * [Sources](#sources)
  * [Maven](#maven)
  * [Java](#java)
- [Project structure](#project-structure)
- [Building](#building)
  * [Maven Commands](#maven-commands)
  * [Maven Profiles](#maven-profiles)
  * [Maven Build Output](#maven-build-output)
  * [Distribution bundles](#distribution-bundles)
- [Running tests](#running-tests)
  * [Joram JMS Testsuite](#joram-jms-testsuite)
  * [JMS TCK](#jms-tck)
  * [Performance Tests](#performance-tests)
  * [Python Tests](#python-tests)

<!-- tocstop -->

## Prerequisites

### Sources

The Qpid project employs [Git distributed version-control system](https://git-scm.com) for tracking changes in source code.

The Git repository can be found at <https://gitbox.apache.org/repos/asf/qpid-broker-j.git>.

The mirror of Git repository exists on GitHub at <https://github.com/apache/qpid-broker-j>.

Changes need to be committed into Apache Git repository. They immediately get propagated into GitHub mirror.
Only members of Qpid project with commit rights can commit the changes. The contributors without commit rights need
to raise [Pull Request](https://help.github.com/en/articles/creating-a-pull-request) on GitHub in order to have
their changes be committed by Qpid project committers.

Git client is required to checkout sources from Git repo

    git clone https://gitbox.apache.org/repos/asf/qpid-broker-j.git qpid-broker-j

For complete reference and documentation on Git please check [Git Book](https://git-scm.com/book/en/v2)

### Maven

Qpid Broker-J project uses [Maven](http://maven.apache.org/) as its build and management tool. Maven version 3 or above
is required for building the project.

You should set the `M2_HOME` environment variable and include its `bin` directory in your `PATH`.

Check that maven is installed on your system by executing the following at your command prompt.

    mvn --version

For full maven install instructions visit [Maven Installation Instructions](http://maven.apache.org/download.cgi#Installation).

For complete reference and documentation on Maven please visit the following online resources.

* [Apache Maven Project](http://maven.apache.org/)
* [Maven: The Complete Reference by Sonatype](http://books.sonatype.com/mvnref-book/reference/public-book.html)


### Java

The build requires a Java 8 or later. You should set the `JAVA_HOME` environment variable and include its `bin` directory
in your `PATH`.

Check java version by executing the following at your command prompt.

    java -version

## Project structure

Qpid Broker-J consists of a number of modules and sub-modules located in their own directories. Each Qpid Broker-J module
has its own POM file (pom.xml) located in its root directory. This file defines the modules version, dependencies and
project inheritance as well as the configuration of the relative maven plugins specific to this module.
The Qpid Broker-J parent pom.xml is located in the root of the project and declares all qpid modules, dependencies,
plugins, etc.

## Building

The project is built by executing maven command in conjunction with pre-defined profiles. For example, the command below
cleans previous build output and install all modules to local repository without running the tests:

    mvn clean install -DskipTests

The following command installs all modules to the local repository after running all the tests:

    mvn clean install

### Maven Commands

Here is a quick reference guide outlining some of the maven commands you can run and what the outcome will be.

Please visit [Introduction To The Maven Lifecycle](https://maven.apache.org/guides/introduction/introduction-to-the-lifecycle.html)
reference guide for full details on all the maven lifecycle phases.

|  Command      | Description                                          | Notes                                  |
|---------------|:-----------------------------------------------------|:---------------------------------------|
| mvn clean     | removes all the files created by the previous build  |Usually means it deletes the contents of the modules `*/target/` directory.|
| mvn validate  | validates that the maven poms are all correct and that no config is missing | Useful to run when making build modifications to ensure consistency.|
| mvn compile   | compiles the source code of the project | This will verify that project dependencies are correct.|
| mvn test      | executes the unit tests | Should not rely on code being packaged or deployed, only unit tests.|
| mvn package   | packages the code into the its distributable formats (jar, zip etc)| Each pom specifies what the distribution format is, default is POM.|
| mvn verify    | verifies that the packaged code is valid| This will run the integration and system tests.|
| mvn install   | installs the package into the local maven repository | This will result in the module being available, locally, as a depedency|
| mvn deploy    | copies the final artifacts to the remote maven repository for sharing | This would happen only when modules ready to be shared with other developers or projects|
| mvn site:stage| creates a staged version of the maven site with all the reports | Staging output defaults to the `*/target/staging/` directory |
| mvn jacoco:report | generates the code coverage report for the tests that have been executed | Test output appears in the `*/target/site/jacoco/` directory |

### Maven Profiles

Maven profiles are used to run tests for the supported protocols and storage options.
The specific profile can be enabled at the command line using option `-P`.
If no profile is selected, `java-mms.1-0` is run by default.

An example below executes integration tests using profile `java-bdb.1-0`.

    mvn verify -Pjava-bdb.1-0

Please visit [Introduction To Maven Profiles](http://maven.apache.org/guides/introduction/introduction-to-profiles.html)
for full details on the use of profiles in a maven build.

Profiles can be listed using command

    mvn help:all-profiles

The test profile names follow the form *java-store.n-n*, where
 *store* signifies the storage module and
 *n-n* the AMQP protocol version number.

For store, the options include:

* *bdb* - Oracle BDB JE
* *dby* - Apache Derby
* *mms* - an in-memory store principally used for testing.

For protocols, the options include:

* *1-0* - AMQP 1-0
* *0-10* - AMQP 0-10
* *0-9-1* - AMQP 0-9-1
* *0-9* - AMQP 0-9

To activate profile for BDB and AMQP 1.0 protocol use:

    mvn verify -Pjava-bdb.1-0

When activating AMQP 0-8..0-10 profiles, it is also necessary to pass the system property *-DenableAmqp0-x*.
For example,

    mvn verify -Pjava-dby.0-9 -DenableAmqp0-x

The table below lists some of profiles defined in parent pom.xml.

|Profile Name    | Description                                            |
|----------------|--------------------------------------------------------|
|apache-release  | Release profile for performing Apache software releases|
|dependency-check|Dependency check to validate project licensing ahead of release|
|java-mms.1-0    | Test profile to run integration tests using AMQP protocol 1.0 against broker with memory message store|
|java-bdb.1-0    | Test profile to run integration tests using AMQP protocol 1.0 against broker with bdb message store|
|java-dby.1-0    | Test profile to run integration tests using AMQP protocol 1.0 against broker with Derby message store|
|java-mms.0-10   | Test profile to run integration tests using AMQP protocol 0-10 against broker with memory message store|
|java-bdb.0-10   | Test profile to run integration tests using AMQP protocol 0-10 against broker with bdb message store|
|java-dby.0-10   | Test profile to run integration tests using AMQP protocol 0-10 against broker with Derby message store|
|java-mms.0-9-1  | Test profile to run integration tests using AMQP protocol 0-9-1 against broker with memory message store|
|java-bdb.0-9-1  | Test profile to run integration tests using AMQP protocol 0-9-1 against broker with bdb message store|
|java-dby.0-9-1  | Test profile to run integration tests using AMQP protocol 0-9-1 against broker with Derby message store|

There is a set of test profiles to run tests against broker with in-memory Derby store, for example

    mvn verify -Pjava-dby-mem.0-10 -DenableAmqp0-x -DskipITs=false

### Maven Build Output

By default the build output for a maven module will appear in the modules `target` directory:
For example, the broker core module output appears in:

    broker-core/target

Depending on the goals specified in the last build, one or all of the following can be found in the `target` directory.

| Location | Description | Note |
|----------|-------------|------|
| */target/|Packaged build artifacts|JAR, WAR, ZIP, etc as well as any release specific artifacts specified in assembly files i.e. release tars|
| */target/surefire-reports/<profile name>|Logs from unit/system test and Surefile summary|Logs files generated by the tests (named TEST-<test-class>-<test-method>.txt). Surefire summary has an .xml extension.|
| */taget/site/| Maven site output|Maven reports generated in HTML format i.e. test reports, code coverage etc|
| */target/staging/|The staged maven site|Local attempt to generate the full maven site|


### Distribution bundles

The broker distribution assemblies can be found beneath:

    apache-qpid-broker-j/target

How to run Qpid broker is covered in [Quick Start Guide](quick-start.md)

## Running tests

Integration tests except for protocol tests are disabled by default.
In order to run all integration tests, they need to be enabled with a flag `-DskipITs=false`, for example

    mvn verify -DskipITs=false

Running all integration tests using BDB and AMQP 1.0 protocol:

    mvn verify -Pjava-bdb.1-0  -DskipITs=false

Perform a subset of tests on the packaged release artifacts without installing:

    mvn verify -Dtest=TestNamePattern* -DfailIfNoTests=false

Execute the tests and produce code coverage report:

    mvn clean test jacoco:report

### Joram JMS Testsuite

The Joram JMS Testsuite is integrated into the Maven build but is disabled by default.
It allows the Joram JMS test suite to be executed using the specified Qpid JMS client against Qpid Broker-J.
The Broker must be running already.

The following clients are supported:

* *qpid-jms-client* - Qpid JMS Client for AMQP 1.0
* *qpid-amqp-1-0-client-jms* - the proof of concept Qpid Client AMQP 1.0 (development ceased after 0.32)
* *jms-client* - Legacy Qpid JMS Client for AMQP 0-x

To use the test suite, first configure the Qpid Broker-J to permit HTTP basic-authentication, then run the following command:

    mvn -f joramtests/pom.xml integration-test -Djoramtests=<jms client identifier>

where `<jms client identifier>` is one of: `qpid-jms-client`, `qpid-amqp-1-0-client-jms`, `jms-client-0-9`, `jms-client-0-10`.

The Maven failsafe plugin will run the tests. Unit output is produced into `joram-tests/target/failsafe-reports`
 and the log from the client is written to `joramtests/target/joramtest.log`.

To run a particular test use the failsafe plugin property `-Dit.test`:

    mvn -f joramtests/pom.xml integration-test -Djoramtests=qpid-amqp-1-0-client-jms  -Dit.test=ConnectionTest

### JMS TCK

The configuration for the JMS TCK is integrated into the Maven build but the profile is disabled by default.
The JMS TCK itself is proprietary and must be provided by the user.
The Broker must be running and HTTP management available. The test suite will automatically set-up/teardown
 the JMS objects required by the TCK.

 * *qpid-jms-client* - Qpid JMS Client for AMQP 1.0
 * *jms-client* - Legacy Qpid JMS Client for AMQP 0-x

To use the test suite, first unpack the JMS TCK into a directory, then invoke tests in the following way:

    mvn post-integration-test -Dtck=qpid-jms-client -f tck/pom.xml -Dtck.directory=/path/to/jmstck/

To run an individual test, use the `tck.test` system property.

    mvn post-integration-test -Dtck=qpid-jms-client -f tck/pom.xml -Dtck.directory=/path/to/jmstck/ -Dtck.test=com/sun/ts/tests/jms/ee/all/sessiontests/SessionTests.java

JMS 2.0 TCK can be run against Qpid Broker-J only with `qpid-jms-client` (for AMQP 1.0)  by specifying profile `jms20-tck`
and location of directory containing expanded TCK. For example,

    mvn post-integration-test -f tck/pom.xml -Pjms20-tck -Dtck.directory=/home/alex/tck/jmstck20 -Dqpid-jms-client-version=0.45.0

### Performance Tests

The Performance test suite is integrated into Maven.
It is bound to the `integration-test` phase and it is activated by the `perftest` system property.
The Broker must be running and HTTP management available. The test suite will automatically set-up/teardown
the JMS objects required for the performance tests.

The following clients are supported:

 * *qpid-jms-client* - Qpid JMS Client for AMQP 1.0
 * *jms-client* - Legacy Qpid JMS Client for AMQP 0-x

To invoke:

    mvn  -f perftests/pom.xml integration-test -Dperftests=jms-client-0-9

Most things can be overridden from system properties. Take a look in the first few lines of the POM.

### Python Tests

The Python Test suite runs against the Qpid Broker-J too but is not currently integrated into Maven.
To run the 0-8..0-10 test suites use the following steps.  It assumes that qpid-python and qpid-cpp are checked out.

Configure the Qpid Broker-J to allow so that the authentication provider permits plain connections.

    curl --user admin:admin -d '{"secureOnlyMechanisms": []}' http://localhost:8080/api/latest/authenticationprovider/passwordFile

Install the Python packages to a suitable location.

    LOCALROOT=~/myroot
    (cd qpid-cpp/management/python && python setup.py install --root ${LOCALROOT})
    (cd qpid-python && python setup.py install --root ${LOCALROOT})
    export PYTHONPATH=${LOCALROOT}/lib/python2.7/site-packages

Run the tests will the following command:

    QPID_HOME=/path/to/my/qpid-broker-j
    cd qpid-python
    ./qpid-python-test -b amqp://guest/guest@localhost:5672 -I${QPID_HOME}/test-profiles/python_tests/Java010PythonExcludes -I${QPID_HOME}/test-profiles/python_tests/JavaPre010PythonExcludes -m qpid_tests.broker_0_10 -m qpid_tests.broker_0_9 -m qpid_tests.broker_0_8 -m qmf.console [<fully qualified test name>]


