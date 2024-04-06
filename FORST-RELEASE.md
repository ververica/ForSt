# ForSt Release Process

## Summary

ForSt releases are a fat jar file that contain the following binaries:
* .so files for linux32 (glibc and musl-libc)
* .so files for linux64 (glibc and musl-libc)
* .so files for linux [aarch64](https://en.wikipedia.org/wiki/AArch64) (glibc and musl-libc)
* .so files for linux [ppc64le](https://en.wikipedia.org/wiki/Ppc64le) (glibc and musl-libc)
* .jnilib file for Mac OSX
* .dll for Windows x64

To build the binaries for a ForSt release, building on native architectures is advised. Building the binaries for ppc64le and aarch64 *can* be done using QEMU, but you may run into emulation bugs and the build times will be dramatically slower (up to x20).

We recommend building the binaries on environments with at least 4 cores, 16GB RAM and 40GB of storage. The following environments are recommended for use in the build process:
* Windows x64
* Linux aarch64
* Linux ppc64le
* Mac OSX

## Build for Windows

For the Windows binary build, we recommend using a base [AWS Windows EC2 instance](https://aws.amazon.com/windows/products/ec2/) with 4 cores, 16GB RAM, 40GB storage for the build.

Firstly, install [chocolatey](https://chocolatey.org/install). Once installed, the following required components can be installed using Powershell:

    choco install git.install jdk8 maven visualstudio2017community visualstudio2017-workload-nativedesktop

Open the "Developer Command Prompt for VS 2017" and run the following commands:

    git clone git@github.com:ververica/ForSt.git
    cd ForSt
    java\crossbuild\build-win.bat

The resulting native binary will be built and available at `build\java\Release\rocksdbjni-shared.dll`. You can also find it under project folder with name `librocksdbjni-win64.dll`.
The result windows jar is `build\java\rocksdbjni_classes.jar`.

There is also a how-to in CMakeLists.txt.

**Once finished, extract the `librocksdbjni-win64.dll` from the build environment. You will need this .dll in the final crossbuild.**

## Build for aarch64

For the Linux aarch64 binary build, we recommend using a base [AWS Ubuntu Server 20.04 LTS EC2](https://aws.amazon.com/windows/products/ec2/) with a 4 core Arm processor, 16GB RAM, 40GB storage for the build. You can also attempt to build with QEMU on a non-aarch64 processor, but you may run into emulation bugs and very long build times.

### Building in aarch64 environment

First, install the required packages such as Java 8 and make:

     sudo apt-get update
     sudo apt-get install build-essential openjdk-8-jdk

then, install and setup [Docker](https://docs.docker.com/engine/install/ubuntu/):

     sudo apt-get install apt-transport-https ca-certificates curl gnupg lsb-release

     curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
     echo "deb [arch=arm64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

     sudo apt-get update
     sudo apt-get install docker-ce docker-ce-cli containerd.io

     sudo groupadd docker
     sudo usermod -aG docker $USER
     newgrp docker

Then, clone the ForSt repo:

     git clone https://github.com/ververica/ForSt.git
     cd ForSt

First, build the glibc binary:

    make jclean clean rocksdbjavastaticdockerarm64v8

**Once finished, extract the `java/target/librocksdbjni-linux-aarch64.so` from the build environment. You will need this .so in the final crossbuild.**

Next, build the musl-libc binary:

     make jclean clean rocksdbjavastaticdockerarm64v8musl

**Once finished, extract the `java/target/librocksdbjni-linux-aarch64-musl.so` from the build environment. You will need this .so in the final crossbuild.**

### Building via QEMU

You can use QEMU on, for example, an `x86_64` system to build the aarch64 binaries. To set this up on an Ubuntu environment:

    sudo apt-get install qemu binfmt-support qemu-user-static
    docker run --rm --privileged multiarch/qemu-user-static --reset -p yes

To verify that you can now run aarch64 docker images:

    docker run --rm -t arm64v8/ubuntu uname -m
    > aarch64

You can now attempt to build the aarch64 binaries as in the previous section.

## Build in PPC64LE

For the ppc64le binaries, we recommend building on a PowerPC machine if possible, as it can be tricky to spin up a ppc64le cloud environment. However, if a PowerPC machine is not available, [Travis-CI](https://www.travis-ci.com/) offers ppc64le build environments that work perfectly for building these binaries. If neither a machine or Travis are an option, you can use QEMU but the build may take a very long time and be prone to emulation errors.

### Building in ppc64le environment

As with the aarch64 environment, the ppc64le environment will require Java 8, Docker and build-essentials installed. Once installed, you can build the 2 binaries:

    make jclean clean rocksdbjavastaticdockerppc64le

**Once finished, extract the `java/target/librocksdbjni-linux-ppc64le.so` from the build environment. You will need this .so in the final crossbuild.**

    make jclean clean rocksdbjavastaticdockerppc64lemusl

**Once finished, extract the `java/target/librocksdbjni-linux-ppc64le-musl.so` from the build environment. You will need this .so in the final crossbuild.**

### Building via Travis

Travis-CI supports ppc64le build environments, and this can be a convenient way of building in the absence of a PowerPC machine. Assuming that you have an S3 bucket called **my-forst-release-artifacts**, the following Travis configuration will build the release artifacts and push them to the S3 bucket:

```
dist: xenial
language: cpp
os:
  - linux
arch:
  - ppc64le

services:
  - docker
addons:
  artifacts:
    paths:
      - $TRAVIS_BUILD_DIR/java/target/librocksdbjni-linux-ppc64le-musl.so
      - $TRAVIS_BUILD_DIR/java/target/librocksdbjni-linux-ppc64le.so

env:
  global:
    - ARTIFACTS_BUCKET=my-forst-release-artifacts
  jobs:
    - CMD=rocksdbjavastaticdockerppc64le
    - CMD=rocksdbjavastaticdockerppc64lemusl

install:
  - sudo apt-get install -y openjdk-8-jdk || exit $?
  - export PATH=/usr/lib/jvm/java-8-openjdk-$(dpkg --print-architecture)/bin:$PATH
  - export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-$(dpkg --print-architecture)
  - echo "JAVA_HOME=${JAVA_HOME}"
  - which java && java -version
  - which javac && javac -version

script:
  - make jclean clean $CMD
```

**Make sure to set the `ARTIFACTS_KEY` and `ARTIFACTS_SECRET` environment variables in the Travis Job with valid AWS credentials to access the S3 bucket you defined.**

**Make sure to avoid signatureV4-only S3 regions to store the uploaded artifacts (due to unresolved https://github.com/travis-ci/artifacts/issues/57). You can just choose the S3 bucket of `us-east-1` region for 100% compatibility.**

**Once finished, the`librocksdbjni-linux-ppce64le.so` and `librocksdbjni-linux-ppce64le-musl.so` binaries will be in the S3 bucket. You will need these .so binaries in the final crossbuild.**


### Building via QEMU

You can use QEMU on, for example, an `x86_64` system to build the ppc64le binaries. To set this up on an Ubuntu environment:

    sudo apt-get install qemu binfmt-support qemu-user-static
    docker run --rm --privileged multiarch/qemu-user-static --reset -p yes

To verify that you can now run ppc64le docker images:

    docker run --rm -t ppc64le/ubuntu uname -m
    > ppc64le

You can now attempt to build the ppc64le binaries as in the previous section.

## Final crossbuild in Mac OSX

Documentation for the final crossbuild for Mac OSX and Linux is described in [java/RELEASE.md](java/RELEASE.md) as has information on dependencies that should be installed. As above, this tends to be Java 8, build-essentials and Docker.

Before you run this step, you should have 5 binaries from the previous build steps:

1. `librocksdbjni-win64.dll` from the Windows build step.
2. `librocksdbjni-linux-aarch64.so` from the aarch64 build step.
3. `librocksdbjni-linux-aarch64-musl.so` from the aarch64 build step.
4. `librocksdbjni-linux-ppc64le.so` from the ppc64le build step.
5. `librocksdbjni-linux-ppc64le-musl.so` from the ppc64le build step.

To start the crossbuild within a Mac OSX environment:

    make jclean clean
    mkdir -p java/target
    cp <path-to-windows-dll>/librocksdbjni-win64.dll java/target/librocksdbjni-win64.dll
    cp <path-to-ppc64le-lib-so>/librocksdbjni-linux-ppc64le.so java/target/librocksdbjni-linux-ppc64le.so
    cp <path-to-ppc64le-musl-lib-so>/librocksdbjni-linux-ppc64le-musl.so java/target/librocksdbjni-linux-ppc64le-musl.so
    cp <path-to-arm-lib-so>/librocksdbjni-linux-aarch64.so java/target/librocksdbjni-linux-aarch64.so
    cp <path-to-arm-musl-lib-so>/librocksdbjni-linux-aarch64-musl.so java/target/librocksdbjni-linux-aarch64-musl.so
    FORST_VERSION=0.1.0-SNAPSHOT PORTABLE=1 ROCKSDB_DISABLE_JEMALLOC=true DEBUG_LEVEL=0 make forstjavastaticreleasedocker

*Note, we disable jemalloc on mac due to https://github.com/facebook/rocksdb/issues/5787*.

Once finished, there should be a directory at `java/target/forst-release` with the ForSt jar, javadoc jar, sources jar and pom in it. You can inspect the jar file and ensure that contains the binaries, history file, etc:

```
$ jar tf forstjni-$(FORST_VERSION).jar
META-INF/
META-INF/MANIFEST.MF
HISTORY-JAVA.md
HISTORY.md
librocksdbjni-linux-aarch64-musl.so
librocksdbjni-linux-aarch64.so
librocksdbjni-linux-ppc64le-musl.so
librocksdbjni-linux-ppc64le.so
librocksdbjni-linux32-musl.so
librocksdbjni-linux32.so
librocksdbjni-linux64-musl.so
librocksdbjni-linux64.so
librocksdbjni-osx.jnilib
librocksdbjni-win64.dl
...
```

*Note that it contains linux32/64.so binaries as well as librocksdbjni-osx.jnilib*.

## Push to Maven Central

For this step, you will need the following:

- The OSX Crossbuild artifacts built in `java/target/forst-release` as above.
- A Sonatype account with access to the staging repository. If you do not have permission, open a ticket with Sonatype, [such as this one](https://issues.sonatype.org/browse/OSSRH-72185).
- A GPG key to sign the release, with your public key available for verification (for example, by uploading it to https://keys.openpgp.org/)

To upload the release to the Sonatype staging repository:
```bash
VERSION=<release version> \
USER=<sonatype user> \
PASSWORD=<sonatype password> \
KEYNAME=<gpg key name> \
PASSPHRASE=<gpg key passphrase> \
java/publish-frocksdbjni.sh
```

Go to the staging repositories on Sonatype:

https://oss.sonatype.org/#stagingRepositories

Select the open staging repository and click on "Close".

The staging repository will look something like  `https://oss.sonatype.org/content/repositories/xxxx-1020`. You can use this staged release to test the artifacts and ensure they are correct.

Once you have verified the artifacts are correct, press the "Release" button. **WARNING: this can not be undone**. Within 24-48 hours, the artifact will be available on Maven Central for use.