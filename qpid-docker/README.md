## Docker Image Example

This is an example on how a Docker Image For Apache Qpid Broker-J based on Eclipse Temurin JRE image can be created.

## Building Container Image

To use an official Apache release in your image run the following command from the qpid-docker directory where
<QPID_RELEASE_VERSION> is the release version you wish to use (e.g. 9.1.0):

```
cd qpid-docker

docker-build.sh --release <QPID_RELEASE_VERSION>
```

This will download the Qpid Broker-J release and copy all the files necessary to build the pre-configured Docker image
and provide you with additional instructions. Follow these instructions to finish building the image you want based on
the provided Docker file or even one of your own.

If you would rather prefer to build the docker image from local Broker-J distribution, build the parent project using
the command

```
mvn clean install -DskipTests=true
```

Navigate to the module 'qpid-docker':

```
cd qpid-docker
```

Execute the command

```
docker-build.sh --local-dist-path <PATH_TO_LOCAL_QPID_DISTRIBUTION>
```

This will copy all the files necessary to build the pre-configured Docker image and provide you with additional
instructions. Follow these instructions to finish building the image you want based on one of the provided Docker file
or even one of your own.

### Container Structure

Broker-J files are copied to the folder /qpid-broker-j \
This folder belongs to user qpid, which is part of the root group. Java process is executed under the qpid user as well.

### Running the Container

Container can be started using following command:
```
docker run -d -p 5672:5672 -p 8080:8080 --name qpid <IMAGE_NAME>
```
There are two ports exposed: 5672 for AMQP connections and 8080 for HTTP connections.

There are following environment variables available when running the container:

| Environment Variable | Description                                                                  |
|----------------------|------------------------------------------------------------------------------|
| JAVA_GC              | JVM Garbage Collector parameters, default value "-XX:+UseG1GC"               |
| JAVA_MEM             | JVM memory parameters, default value "-Xmx300m -XX:MaxDirectMemorySize=200m" |
| JAVA_OPTS            | Further JVM parameters, empty by default                                     |

#### Container Volume

The image will use the directory /qpid-broker-j/work to hold the configuration and the data of the running broker.
To persist the broker configuration and the data outside the container, start container with the volume mapping:

```
docker run -d -p 5672:5672 -p 8080:8080 -v <BROKER_DIRECTORY_ON_HOST>:/qpid-broker-j/work --name qpid <IMAGE_NAME>
```
or
```
podman run -d -p 5672:5672 -p 8080:8080 -v <BROKER_DIRECTORY_ON_HOST>:/qpid-broker-j/work:Z --name qpid <IMAGE_NAME>
```

### Stopping the Container

Running container can be stopped using following command:
```
docker stop qpid
```

### Broker Users

Default configuration provides a preconfigured broker user, having read and write access to all broker objects:
- admin (default password 'admin')

Username of the 'admin' user can be overridden be providing the variable QPID_ADMIN_USER on start, and the default 
password of the 'admin' user can be overridden be providing the variable QPID_ADMIN_PASSWORD on start:

```
docker run -d -p 5672:5672 -p 8080:8080 -v <BROKER_DIRECTORY_ON_HOST>:/qpid-broker-j/work -e QPID_ADMIN_USER=myuser -e QPID_ADMIN_PASSWORD=mypassword --name qpid <IMAGE_NAME>
```

Further broker users as well as other broker objects (queues, exchanges, keystores, truststore, ports etc.)
can be created via HTTP management interface. Description of the broker REST API can be found in broker book (chapter 6.3).

To change user password following command can be used:

```
curl -d '{"password": "<NEW_PASSWORD>"}' http://admin:admin@localhost:8080/api/latest/user/plain/<USERNAME>
```

## Broker Customization

To customize broker before building the container image, its configuration files may be edited to start broker with
queues, exchanges, users or other objects.

The file config.json contains definitions of the broker objects and references a file containing definitions
of virtualhost objects (exchanges and queues).

It may be helpful first to create broker objects needed via broker web GUI or via REST API, and then investigate the
configuration files and copy the appropriate definitions to the configuration files used for container image creation.

An example of the default initial configuration JSON file is provided in broker book (chapter 5.7).

### Exchanges

To create exchanges a JSON element "exchanges" should be created containing an array of single exchange definitions:

```
"exchanges" : [ {
      "name" : "amq.direct",
      "type" : "direct"
    }, {
      "name" : "amq.fanout",
      "type" : "fanout"
    }, {
      "name" : "amq.match",
      "type" : "headers"
    }, {
      "name" : "amq.topic",
      "type" : "topic"
    }, {
      "name" : "request.QUEUE1",
      "type" : "topic",
      "durable" : true,
      "durableBindings" : [ {
        "arguments" : { },
        "destination" : "QUEUE1",
        "bindingKey" : "#"
    } ],
    "unroutableMessageBehaviour" : "REJECT"
  }
]
```

Information about exchanges, their types and properties can be found in broker book (chapter 4.6).

Please note that each virtualhost pre-declares several exchanges, described in the broker book (chapter 4.6.1).

### Queues

To create queue a JSON element "queues" should be created containing an array of single queue definitions:

```
"queues" : [ {
    "name" : "QUEUE1",
    "type" : "standard",
    "durable" : true,
    "maximumQueueDepthBytes" : 6144000,
    "maximumQueueDepthMessages" : 6000,
    "messageDurability" : "ALWAYS",
    "overflowPolicy" : "REJECT"
  }, {
    "name" : "QUEUE2",
    "type" : "standard",
    "durable" : true,
    "maximumQueueDepthBytes" : 6144000,
    "maximumQueueDepthMessages" : 6000,
    "messageDurability" : "ALWAYS",
    "overflowPolicy" : "REJECT"
  }
]
```

Information about queues, their types and properties can be found in broker book (chapter 4.7).

### Users

Users can be defined in an authentication provider. Authentication providers are defined on broker level (file config.json).

Information about authentication providers, their types and properties can be found in broker book (chapter 8.1).

Examples for most commonly used authentication providers can be found below.

#### Anonymous Authentication Provider

```
"authenticationproviders" : [ {
    "name" : "anon",
    "type" : "Anonymous"
  } ]
```
For additional details see broker book (chapter 8.1.5).

#### Plain Authentication Provider

```
"authenticationproviders" : [{
    "name" : "plain",
    "type" : "Plain",
    "secureOnlyMechanisms" : [],
    "users" : [ {
      "name" : "admin",
      "type" : "managed",
      "password" : "<PASSWORD>"
    } ]
  } ]
```

For additional details see broker book (chapter 8.1.7).

#### ACL Rules

The ACL rules for users are defined in file broker.acl following the syntax:

```
ACL {permission} {<group-name>|<user-name>|ALL} {action|ALL} [object|ALL] [property=<property-values>]
```

The predefined broker.acl file contains permissions for the 'admin' user:

```
# account 'admin' - enabled all actions
ACL ALLOW-LOG QPID_ADMIN_USER ALL ALL
```

For additional details see broker book (chapter 8.3.2).

### Overriding Broker Configuration

Customized configuration for the Broker-J instance can be used by replacing the files residing in the work folder with 
the custom ones, e.g. config.json or default.json. Put the replacement files inside a folder and map it as a volume to:

```
docker run -d -p 5672:5672 -p 8080:8080 -v <DIRECTORY_ON_HOST>:/qpid-broker-j/work-override:Z --name qpid <IMAGE_NAME>
```

The contents of work-override folder will be copied over to work folder first time after the instance creation so that 
the broker will start with user-supplied configuration.