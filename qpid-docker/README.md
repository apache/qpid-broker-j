## Docker Image Example

This is an example on how a Docker Image For Apache Qpid Broker-J based on Eclipse Temurin JRE image can be created.

## Building Container Image

Build the parent project using command

```
mvn clean install -DskipTests=true
```

Navigate to the module 'qpid-docker':

```
cd qpid-docker
```

Execute command

```
docker-build.sh --local-dist-path <PATH_TO_LOCAL_QPID_DISTRIBUTION>
```

This will copy all the files necessary to build the pre-configured Docker image and provide you with additional 
instructions. Follow these instructions to finish building the image you want based on one of the provided Docker file 
or even one of your own.

If you would rather use an official Apache release in your image rather than a local release then run the following 
command from the qpid-docker directory where <QPID_VERSION> is the release version you wish to use (e.g. 9.1.0):

```
docker-build.sh --qpid-version <QPID_VERSION>
```

This will download the Qpid Broker-J release and copy all the files necessary to build the pre-configured Docker image
and provide you with additional instructions. Follow these instructions to finish building the image you want based on 
the provided Docker file or even one of your own.

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

Before starting the container a volume should be created:

```
docker volume create --driver local --opt device=<PATH_TO_FOLDER_ON_HOST_MACHINE> --opt type=none --opt o=bind qpid_volume
```

Then container using created volume can be started as follows:

```
docker run -d -p 5672:5672 -p 8080:8080 -v qpid_volume:/qpid-broker-j/work --name qpid <IMAGE_NAME>
```
or
```
podman run -d -p 5672:5672 -p 8080:8080 -v qpid_volume:/qpid-broker-j/work:Z --name qpid <IMAGE_NAME>
```
When container is started for the first time, configuration files become available in the mapped folder being persisted 
after subsequent restarts.

### Broker Users

Default configuration provides two preconfigured broker users:
- admin (password 'admin')
- guest (password 'guest')

User 'admin' has read and write access to all broker objects.
User 'guest' has read-only access to all broker objects, can not send or consume messages.

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

### Exchanges

To create exchanges a JSON element "exchanges" should be created containing an array of single exchange definitions:

```
"exchanges" : [ {
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
  } ],
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
    }, {
      "name" : "guest",
      "type" : "managed",
      "password" : "<PASSWORD>"
    } ]
  } ],
```

For additional details see broker book (chapter 8.1.7).
