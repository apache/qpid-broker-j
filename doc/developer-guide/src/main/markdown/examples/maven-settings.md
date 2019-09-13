# Maven settings with GPG keys

An example of maven `settings.xml` illustrating how to configure gpg keys

```xml
<settings>
  <servers>
    <server>
      <id>apache.snapshots.https</id>
      <username>YOUR_APACHE_SVN_USERNAME</username>
      <password>YOUR_APACHE_SVN_PASSWORD</password>
    </server>
    <server>
      <id>apache.releases.https</id>
      <username>YOUR_APACHE_SVN_USERNAME</username>
      <password>YOUR_APACHE_SVN_PASSWORD</password>
    </server>

    <!-- optional -->
    <server>
      <id>gpg.passphrase</id>
      <passphrase>{XXXX}</passphrase>
    </server>
 </servers>

  <profiles>
    <profile>
      <id>apache-release</id>
      <properties>
        <gpg.keyname>A2889D83</gpg.keyname>
        <gpg.executable>gpg2</gpg.executable>
      </properties>
    </profile>
  </profiles>
</settings>
```
