# IDE integration

Nowadays, most of the team chose [IntelliJ](https://www.jetbrains.com/idea).

## Integration with IntelliJ

To setup the project, simply import Qpid Broker-J parent pom.xml as a project.

## Running the Broker within Intellij

After importing maven projects create Application configuration following instructions below

1. Create a new Application run configuration called "Qpid Broker"
   * Classname org.apache.qpid.server.Main
   * Classpath of module: qpid-broker
   * Save the new run configuration.
2. Go into the module settings of qpid-broker and add an additional classpath entry pointing to the dojo-x.x.x-distribution.zip.
   It is easiest to point to a location in the local maven repo e.g. ~/.m2/repository/org/dojotoolkit/dojo/x.x.x/dojo-x.x.x-distribution.zip.
   (This manual step is required to workaround https://issues.apache.org/jira/browse/MNG-5567).
3. Specify the following JVM options for the Qpid Broker Application
 * -Dprofile.virtualhostnode.context.blueprint='{"type":"ProvidedStore","globalAddressDomains":"${dollar.sign}{qpid.gloabalAddressDomains}"}'
 * -Dqpid.globalAddressDomains=[]
 * -DQPID_WORK=/path/to/Qpid/work/directory

## Running System Tests within Intellij

Running system tests from within IntelliJ requires a bit of fiddeling.
You first create a template configuration which you then need to adapt every time you want to run a different systest.
This means this method does not support running the entire test suite.

1. Add a new JUnit Configuration
   * "Run" --> "Edit Configurations..."
   * Press the plus sign at the top left to add a new configuration and select "JUnit"
   * Change the name to "Systest"
   * Add the following to the "VM options"
     * -Dprofile.virtualhostnode.context.blueprint='{"type":"ProvidedStore","globalAddressDomains":"${dollar.sign}{qpid.gloabalAddressDomains}"}'
     * -Dqpid.globalAddressDomains=[]
     * -DQPID_WORK=/path/to/Qpid/work/directory
2. When you want to actually run a specific test, copy the fully qualified class name of the test into the "Class"
  field of this configuration before executing it.

## IDE Code Style

There is a code style file for the IntelliJ IDE located at /etc/IntelliJ_Qpid_Style.xml in the source tree.
All Qpid Java developers should use this or an equivalent style for their IDE.
Refer [Qpid Broker-J Coding Standards](code-guide.md) for more details about code style.
