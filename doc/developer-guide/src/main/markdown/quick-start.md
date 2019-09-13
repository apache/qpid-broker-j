# Quick Start Guide

## Prerequisites

The following tools are needed to build Qpid Broker-J

 * Git client
 * JDK 1.8 or above; you should set the `JAVA_HOME` environment variable and include its bin directory in your `PATH`.
 * Maven 3.0 or above; you should set the `M2_HOME` environment variable and include its bin directory in your `PATH`.

## Getting sources

Checkout sources from git repository

    git clone https://gitbox.apache.org/repos/asf/qpid-broker-j.git

## Building

Navigate into checkout directory and execute maven command

    mvn clean package

## Running tests

Unit tests are executed as part of maven lifecycle phase `test` and integration tests are executed as part of maven
phase `verify`. An execution of integration tests is skipped by default (except for protocol tests). In order to run
all integration tests a flag `skipITs` needs to be set to `false`. For example,

    mvn verify -DskipITs=false

The command above executes both unit and integration tests. To run only unit tests use the command below.

    mvn test

## Distribution bundle

Broker distribution bundle is built as part of maven lifecycle phase `package`, for example

    mvn clean package -DskipTests

The broker distribution assemblies will then be found beneath:

    apache-qpid-broker-j/target

## Running the Broker

For full details, see chapter `Getting Started` in [broker documentation](http://qpid.apache.org/components/broker-j/index.html).
The brief instructions are repeated here.

### Expand the broker distribution bundle

Expand the assembly produced by the maven lifecycle stage `package`.

On Linux/Unix:

    tar xvfz apache-qpid-broker-j/target/apache-qpid-broker-j-x.x.x-SNAPSHOT-bin.tar.gz -C /target/directory

On Windows:

    Expand zip apache-qpid-broker-j/target/apache-qpid-broker-j-x.x.x-SNAPSHOT-bin.zip


### Declare QPID_WORK environment variable

The Qpid Broker-J stores its configuration and message data in working directory. The path to working directory can be
specified using environment variable `QPID_WORK`.

Examples of declaration of environment variable `QPID_WORK` are given below.

On Linux/Unix:

    export QPID_WORK=${HOME}/my-qpid-work

On Windows:

    set QPID_WORK=%APPDATA%\Qpid

### Start broker

Navigate to directory where Qpid broker bundle was expanded and execute Qpid broker start-up script

On Linux/Unix:

    ./java-broker/x.x.x/qpid-server

On Windows:

    .\java-broker\x.x.x\qpid-server.bat

### Connecting to the Broker

By default, the Broker listens on port 5672 for AMQP and 8080 for http management.  The default username is 'guest'
and password is 'guest'.

To get to the management console, point a browser to `http://localhost:8080`

## Reporting defect or requesting new feature or improvement

[JIRA](https://issues.apache.org/jira/issues/?jql=project%20%3D%20QPID%20AND%20component%20%3D%20Broker-J%20order%20by%20created%20DESC)
is used as an issue tracking system for Qpid Broker-J. A JIRA ticket needs to be raised for the issue.

## Submitting changes

The changes can be submitted as pull request against github mirror of Apache Qpid Broker-J repo:

    https://github.com/apache/qpid-broker-j

## How to contribute changes to Qpid Broker-J

Here is a set of simple instructions to follow in order to contribute changes.
Please note, that changes need to be implemented on master branch first before they can be ported into specific version support branch.

* Raise JIRA ticket
* Fork github mirror of broker-j repository, if it is not forked yet
* Create working branch from master in forked repo
* Implement required changes and unit/integration tests
* Verify that implementation follows [Qpid code standard](code-guide.md) and [clean code practices](https://en.wikipedia.org/wiki/SOLID)
* Verify that all tests are passing locally on a developer machine
* Commit the changes into working branch
* Create pull request in github mirror of broker-j repository
* Verify that all tests executed for PR by project Continuous Integration tools are still passing
* Address comments submitted by pull request reviewers if any and applicable
* The reviewed changes needs to be applied into the master branch by project committer
* Close the JIRA ticket
