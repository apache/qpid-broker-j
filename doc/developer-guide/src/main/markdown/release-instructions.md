# Releasing Qpid Broker-J

This document summarizes the steps of release process for Qpid Broker-J

<!-- toc -->

- [Pre-Requirements](#pre-requirements)
  * [PGP](#pgp)
  * [Maven](#maven)
  * [Java](#java)
  * [Git](#git)
- [Release Steps](#release-steps)

<!-- tocstop -->

## Pre-Requirements

The release process is based on [Apache Release Policy](http://www.apache.org/dev/release.html),
 [Release Signing](http://www.apache.org/dev/release-signing.html) and
 [Publishing of Maven Documents](http://www.apache.org/dev/publishing-maven-artifacts.html#dev-env).

### PGP

Release artifacts needs to be signed. GNU Privacy Guard from [OpenPGP](https://www.openpgp.org) is used to sign on
 Apache. Please, refer [Release Signing](http://www.apache.org/dev/release-signing.html) about PGP public key
 cryptography. The document provides basic information about release signing and contains links to various resources
 about PGP public key cryptography and how to use GNU Privacy Guard to generate and sign the PGP keys. Install
 [GNU Privacy Guard](http://www.gnupg.org), generate keys following steps provided here and upload public keys to
 keyservers. After publishing keys, login into <https://id.apache.org> and enter PGP key fingerprint(s),
 then the corresponding keys will be made available under <https://people.apache.org/keys/> within a few hours.
 Additionally, qpid project requires adding a public key into <https://dist.apache.org/repos/dist/release/qpid/KEYS>.

### Maven

Maven is used to build and manage Qpid Java project. 3.x version of maven needs to be installed and the development
 environment should be setup as described at
 [Publishing of Maven Artifacts](http://www.apache.org/dev/publishing-maven-artifacts.html#dev-env).

Please, note that repository id (server id) in `settings.xml` should be **apache.releases.https**. Using different id
would result in failures to publish release artifacts into staging maven repo.

### Java

JDK 1.8 is required to compile java classes. Install latest 1.8 JDK. At the moment of writing this document JDK version
1.8.0_192  was the latest one.

### Git

Sources are kept in a Git repository. Thus a git client is required.

## Release Steps

1.  Checkout Qpid Broker-J Sources
    * For new major/minor release; checkout sources master

        git clone https://gitbox.apache.org/repos/asf/qpid-broker-j.git qpid-broker-j
    * For bugfix release
        * if support branch does not exist, cut the support branch and set the correct version in maven.
          For example, here are the  commands to cut branch '8.0.x'

                git clone https://gitbox.apache.org/repos/asf/qpid-broker-j.git qpid-broker-j
                cd ./qpid-broker-j
                git checkout -b 8.0.x
                git push -u origin 8.0.x
        * if branch exists, checkout branch sources

                git clone -b 8.0.x https://gitbox.apache.org/repos/asf/qpid-broker-j.git 8.0.x
                cd 8.0.x
2.  Run RAT tool to verify that all source files have license headers

        mvn  apache-rat:check
3.  Add license headers to the files which do not have licenses. Update RAT excludes if required.
4.  Check that images don't have a non-free ICC profile.

        find . -regextype posix-extended -iregex '.*\.(jpg|png|ico|tif)' -exec sh -c 'identify -verbose "$0" | grep -i copyright && echo "$0"' {} \;
5.  Check that build completes successfully using profile **apache-release**

        mvn clean install -Papache-release -DskipTests
    The gpg plugin will prompt for the password of PGP  signing key. If password is asked for every release artifact,
    then gpg2 should be configured to use. The easiest way to configure gpg2 is to add an active profile with pgp
    plugin settings into `settings.xml` as illustrated in [maven settings example](examples/maven-settings.md).
6.  Verify third party licenses

        mvn -Pdependency-check prepare-package -DskipTests
    The check should finish successfully. Otherwise, dependencies with not compliant licenses should be resolved
    before taking next release step.
7.  Check JIRA system for any unresolved JIRAs for the release and notify assigned developers to take actions
    on uncompleted JIRAs.
8.  Build RC
    * If it is not a first RC, remove previous tag from git

            git push --delete origin x.y.z
            git tag --delete x.y.z # deletes local tag
    * Cut the tag using maven:prepare

            mvn release:clean
            mvn release:prepare -Papache-release,java-mms.1-0  -DautoVersionSubmodules=true -DpreparationGoals=test
      Release plugin will ask about new release version, tag name and new development version.
      Enter the same values for version and tag.
      On successful execution a tag with a provided name will be created, the tag version will be set to the specified
      release version and development version on the branch will be changed to the provided one.
    * Build the RC and publish release artifacts into maven staging repo

            mvn release:perform -Papache-release,java-mms.1-0 -Darguments="-DskipTests"
    * The staging maven repository needs to be closed. Log into
      [Apache Nexus UI](https://repository.apache.org/#stagingRepositories), select the repository under
      **Staging Repository** and click `Close` button to close staging repository for any publishing of artifacts.
      After closing, a permanent link to the staging repository will be available.
    * Copy source and binary bundles and their signatures/checksum files from the nexus staging repository into
      qpid dev staging area at <https://dist.apache.org/repos/dist/dev/qpid/broker-j/> under the sub-folder with
      the same name as tag. Binary bundles  and their signatures/checksum files need to be put into sub-folder
      with name binaries. (Not doing so would break the site). Manually rename the source artifact to keep with
      the source artifact name consistent.

            version=x.y.z
            root=https://repository.apache.org/content/repositories/orgapacheqpid-####
            mkdir binaries

            for i in "" ".asc"; do
                curl -O $root/org/apache/qpid/apache-qpid-broker-j/${version}/apache-qpid-broker-j-$version-src.tar.gz$i
                if [[ "$i" == "" ]]; then
                    sha512sum apache-qpid-broker-j-$version-src.tar.gz > apache-qpid-broker-j-$version-src.tar.gz.sha512
                fi
            done

            cd binaries

            for i in "" ".asc"; do
                for j in "tar.gz" "zip"; do
                    curl -O $root/org/apache/qpid/apache-qpid-broker-j/${version}/apache-qpid-broker-j-$version-bin.$j$i
                done
            done

            for j in "zip" "tar.gz"; do
              sha512sum apache-qpid-broker-j-$version-bin.$j > apache-qpid-broker-j-$version-bin.$j.sha512
            done

            for j in "zip" "tar.gz"; do
               sha512sum -c apache-qpid-broker-j-$version-bin.$j.sha512
               gpg --verify apache-qpid-broker-j-$version-bin.$j.asc
            done

            cd ..

            sha512sum -c apache-qpid-broker-j-$version-src.tar.gz.sha512
            gpg --verify apache-qpid-broker-j-$version-src.tar.gz.asc
    * Send an email into **users@qpid.apache.org** about RC availability with links to the maven staging repository
      and qpid dev staging area folder containing source and binary bundles.
9.  If RC is OK and voting passes, publish release artifacts:
    * send a email to **users@qpid.apache.org** to close the vote. This should include the number of binding and
      non-binding votes, the result, and a link to the voting thread (use Apache services -
      [this](https://mail-archives.apache.org/mod_mbox/qpid-users/) or
      [this](https://lists.apache.org/list.html?users@qpid.apache.org)).
    * promote maven staging artifacts from staging repository into the world by pressing Release button in
      [Apache Nexus UI](https://repository.apache.org/#stagingRepositories).
    * copy source and binary bundles and their signatures/checksum files form dev staging are into
      release distribution area.

            svn cp -m "Publish x.y.z release artifacts" https://dist.apache.org/repos/dist/dev/qpid/broker-j/x.y.z-rc1 \
             https://dist.apache.org/repos/dist/release/qpid/broker-j/x.y.z
      If voting does not pass, resolve found issues, drop staging repository, delete svn tag and repeat instructions
      from step 8 until voting passes.
10. Wait for 24 hours after closing the vote
11. Update Qpid web site pages for new release of Qpid Broker-J component and publish new release documentation
    following instructions [in site README.md](https://gitbox.apache.org/repos/asf?p=qpid-site.git;a=blob;f=README.md).
    Here are sample commands which could be used to create 7.0.0 release pages on the website based on the 7.0.0 tag.

        git clone https://gitbox.apache.org/repos/asf/qpid-site.git site
        cd ./site
        make gen-broker-j-release RELEASE=7.0.0 ISSUES_RELEASE=qpid-java-broker-7.0.0
        vim ./input/releases/qpid-broker-j-7.0.0/release-notes.md # headline major enhancements and new features
        vim ./input/_transom_config.py              # Update the current release pointer
        vim ./input/releases/index.md               # Add current release, move the previous
        make render
        make check-links
        make publish
        git add input/
        git add content/
        git commit -m "Update site for Qpid Broker-J release 7.0.0"
        git push
12. Mark release as released in JIRA and close any unresolved JIRAs if required
13. Send the release notification email into  **users@qpid.apache.org**, **dev@qpid.apache.org** and
    **announce@apache.org**.
    * Note, In order to send messages into **announce@apache.org** one need to send the mail with the **From** field
     set to one's apache.org address. Gmail can be set-up to do so via use of the ASF mail relay.
     The details how to set up apache account to use ASF mail relay can be found in the following resources
      * <https://blogs.apache.org/infra/entry/committers_mail_relay_service>
      * <https://reference.apache.org/committer/email#sendingemailfromyourapacheorgemailaddress>
      * <http://gmailblog.blogspot.co.uk/2009/07/send-mail-from-another-address-without.html>
14. Remove the previous release binaries from <https://dist.apache.org/repos/dist/release/qpid/broker-j>
    when a new one is announced.
15. Create jenkins jobs for new major/minor version if required
