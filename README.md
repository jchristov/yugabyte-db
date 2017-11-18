<img src="https://www.yugabyte.com/images/yblogo_whitebg.3fea4ef9.png" align="center" height="56" alt="YugaByte DB"/>

**Table of Contents**

- [What is YugaByte](#what-is-yugabyte)
- [Supported APIs](#supported-apis)
- [Installation](#installation)
- [Getting Started](#getting-started)
- [Core Concepts](#core-concepts)
    - [Single Node](#single-node)
    - [Universe](#universe)
    - [TServer and Master](#tserver-and-master)
    - [Data Persistence](#data-persistence)
    - [YugaByte Query Layer](#yugabyte-query-layer)
    - [Data Replication](#data-replication)
- [Build Prerequisites](#build-prerequisites)
	- [CentOS 7](#centos7)
	- [Mac OS X](#macosx)
	- [All platforms: Java prerequisites](#all-platforms-java-prerequisites)
	- [Java Driver](#java-driver)
- [Building YugaByte code](#building-yugabyte-code)
- [Running a Local Cluster](#running-a-local-cluster)
- [Comparison with other Databases](#comparison-with-other-databases)
- [Reporting Issues](#reporting-issues)
- [Contributing](#contributing)
- [License](#license)

##  YugaByte DB
YugaByte DB is an open source, cloud-native database for mission-critical enterprise applications. It is meant to be a system-of-record/authoritative database that applications can rely on for correctness and availability. It allows applications to easily scale up and scale down in the cloud, on-premises or across hybrid environments without creating operational complexity or increasing the risk of outages.

## Supported APIs
YugaByte is compatible with the following wire protocols-
* Apache Cassandra Query Language
* Redis APIs
*  SQL support is on the roadmap

## Installation
* Building from Source
    * Follow the instructions [here](#building-yugabyte-code) for building from source.
* Installing prebuilt packages
    * Community Edition -
        Follow instruction at https://docs.yugabyte.com/quick-start/install/ for installing YugaByte community edition for your platform.
    * Enterprise Edition -
        To install the enterprise edition of our public beta, please schedule a demo at sales@yugabyte.com

## Getting Started
Here are a few resources for getting started with YugaByte:
* [Community Edition Quick Start](http://docs.yugabyte.com/community-edition/quick-start/) to
  get started with YugaByte using a pre-built YugaByte Community Edition package.
* See [YugaByte Documentation](http://docs.yugabyte.com/) for architecture,
  production deployment options and languages supported. In particular, see
  [Architecture / Concepts](http://docs.yugabyte.com/architecture/concepts/) and
  [Architecture / Core Functions](http://docs.yugabyte.com/architecture/core-functions/) sections.
* See [www.yugabyte.com](https://www.yugabyte.com/) for general information about YugaByte.
* Check out the [YugaByte Community Forum](http://forum.yugabyte.com) and post your questions
  or comments.

## Core Concepts

### Single Node -
  Every node of YugaByte DB is comprised of 2 layers, YBase and YQL. YBase, the core data fabric, is a highly available, distributed system with strong write consistency, tunable read consistency, and an advanced log-structured row/document-oriented storage model with several optimizations for handling ever-growing datasets efficiently. YQL is the upper/edge layer that has the API specific aspects - for example, the server-side implementation of Apache Cassandra and Redis protocols, and the corresponding query/command compilation and run-time (data type representations, built-in operations, etc.).
  Read More at https://docs.yugabyte.com/architecture/concepts/single-node/
  <p align="center">
    <img src="https://docs.yugabyte.com/images/architecture.png" width="320" height="190"/>
  </p>
### Universe -
  A universe is a group of nodes (VMs, physical machines or containers) that collectively function as a locally/globally distributed, highly available and resilient database. Read More at https://docs.yugabyte.com/architecture/concepts/universe/
  <p align="center">
    <img src="https://lh3.googleusercontent.com/o-zmMrgRsvNT22BrRYtCzhblHSXFXY3uqu5znmvX_HNqigeG3KgFlwqHgrGclitcdlP1j7PsefxQis5pJ87PSNrKqs65EPAJxq5lSJ0O1viFbEh0VLJZvkTpwy4TNSWZQKd3oBx4c9cJA9OvPSF16q7EHZsDM7YZD_UiAeP2YIHavE-beSMh3K_caZAYql3GkaF0WeCZ5rpqPYaEzVtBz42hQewfZDnd_ZvJ5HPXDuV7hSECg8aRdF8TL2UHwy-jx4ocPqSE-VuquwjcUBypKymFEhr98EHHnEsNRK432GXKMlobrDiV-PHLNNerX1HFlrLn4L3dRc30dmFSJOVEG5BLuHlr_CMfGwDttx4zncff6HVrFLpd1k357g_NpXSmE5Dyp7mN7nC3S6U-BLHFMAr-SaAmuDFgdp7lZEpiCTV36G6bI0qIRFpptli31vdnjJ57lAzd4TBvsk1r4kpYqpVz4pSKwqyzoWr0c2eRLiP-NDBxuQ2WHIeiyJ7l8BY0RTj1DgqNLJl9cwoglgbgxQs1RYzlB8Vo0NbiksiTDSOztqRO-zyowetsmB6i5fJPZKeETqbBoQT60Z_xrIa0mwOVPBW50a0f2ayNZq3l=w2680-h1670-no" width="320" height="190"/>
  </p>
### TServer and Master -

  __YB TServer__

  The YB-TServer (aka the YugaByte Tablet Server) processes are responsible for hosting/serving user data.
  <p align="center">
    <img src="https://lh3.googleusercontent.com/2nx6x2q5gXZpivpexyfQhETxq0cDWjsjzuXn_FrjRUOnMlbgB5Od6DP4Yow_-OW34wb8RhK8sitVb8UViwCtFthVC64Ji3kvPPJ3zMFoXuzcEU3DMhLIKLv50qnWq-pkgfaTv-sFdtwMxDMziGeG0cDGjr67chKv5D0MLtw8tjELEeye30lQYtN3Ey9uh-_Muky5lWh4aWyXStuoMGhDR0MWpY8d_32Sl7pnAbWlJ5P2snNXlzNm5nJmyXmUJhYJxKW31DbUz0eUTE4rk5WvXnjU8ANA47RehTFlirscQd6zXAe9psKWyFuGI1wAMtLBvbWgfXKmDetQRIFX2lzNimE0rEXZOZ9RbE89UhNdKraM40YnEOcfvCbWVUeiqUbrRiCvx_46Ly_PmXWAvuV2qlCDiZRL1Ti9Jp_tFofUsEbl6Uei1rnuxlbp9aP2wJJi4Z7ako5QBhyYsk8O-QdFgEzbe7xrldBU-A4R0lWSSPktqQlwOcYJv_80MdFtIFlUQy7Ts-vw59bS1frYL-L_joaVhEamNc5R0rLQPIc_2baAvDkKmCt2nEd9Nc1I0HeP-XBmADtq-vHCHC9eiTpwF5kD7dKMd7fLRfs2WgIG=w2854-h1800-no" width="320" height="190"/>
  </p>

  __YB Master__

  The YB-Master (aka the YugaByte Master Server) is the keeper of system meta-data/records, also responsible for coordinating background operations (such as load-balancing or initiating re-replication of under-replicated data) .
  <p align="center">
    <img src="https://lh3.googleusercontent.com/vfR1udgQfpmQ5tYx2RCEN5IkiztwlA1Bd8gDYnEzxeRH-cuIqqcwDQlaIKVPVan5mC0hbkRvvLZdUey8PgxpeqtkDKU2bfJTbk-rc26jHSUzysRGMA-1BOXLiUjhDFX3EPa3ZUd5onU34X6azbcvYsejNNsKVGXYEFEhKFYAXLho2RzglyLEPGJ6VtKn07eyV09LHXwttc_B34g0VKDtYOKLKs4FPgfxHxH13vud5Ik9oAxTjDspjAbLa2WJE5CiUQASBRTpVY7IaESgdfKOPOy3Kgx4pvh5pPhYAifqxsP5b8-tituWUrohL9vyKdyjwgpASau_uos7OrGNWYYo6S2K12xn9M5fFSH_dUP8zzo5DG7Up5W5Ni5JnzLJK1tZX-8SrCQzS7YmxDaSVMTNi9A3T5hUp6bi8mq3orLzgSJRUFuI-Yqlr57awE4qulFxIr38hKnK7R1cS7UQtvOJ8NoLUs0qCNxi8Fd2AhTBHQxHZWPNc0nOZgq5tFzpeUHX6NYdUESB8hb5L0wSNqEyDHCt0sWdRUBlvYQDZCtKXfgJJnf1dxXaNaVxRSS0Y7bmDi8topgL9QsFFMKHlYLm9jbiNpfNqby6qz0eRjnP=w2880-h1800-no" width="320" height="190"/>
  </p>
  Read More at https://docs.yugabyte.com/architecture/concepts/universe/
### Data Persistence -
DocDB is YugaByte’s Log Structured Merge tree (LSM) based storage engine. Once data is replicated via Raft across a majority of the tablet-peers, it is applied to each tablet peer’s local DocDB.
 Read More at https://docs.yugabyte.com/architecture/concepts/persistence/

### YugaByte Query Layer -
The YQL layer implements the server-side of multiple protocols/APIs that YugaByte supports. Currently, YugaByte supports Apache Cassandra & Redis wire-protocols natively, and SQL is in the roadmap.
<p align="center">
<img src="https://docs.yugabyte.com/images/cluster_overview.png" width="320" height="140"/>
</p>
Read More at https://docs.yugabyte.com/architecture/concepts/yql/

### Data Replication -
Replication of data between the tablet-peers is strongly consistent using a custom implementation of the RAFT consensus algorithm. To achieve a Fault Tolerance of k nodes, a universe has to be configured with a RF of (2k + 1).
Read More at https://docs.yugabyte.com/architecture/concepts/replication/
<p align="center">
<img src="https://docs.yugabyte.com/images/raft_replication.png" width="320" height="140"/>
</p>

## Build Prerequisites

### CentOS 7

CentOS 7 is the main recommended development and production platform for YugaByte.

Update packages on your system, install development tools and additional packages:

```bash
sudo yum update
sudo yum groupinstall -y 'Development Tools'
sudo yum install -y ruby perl-Digest epel-release cyrus-sasl-devel cyrus-sasl-plain ccache
sudo yum install -y cmake3 ctest3
```

Also we expect `cmake` / `ctest` binaries to be at least version 3. On CentOS one way to achive
this is to symlink them into `/usr/local/bin`.

```bash
sudo ln -s /usr/bin/cmake3 /usr/local/bin/cmake
sudo ln -s /usr/bin/ctest3 /usr/local/bin/ctest
```

You could also symlink them into another directory that is on your PATH.

We also use [Linuxbrew](https://github.com/linuxbrew/brew) to provide some of the third-party
dependencies on CentOS. We install Linuxbrew in a separate directory, `~/.linuxbrew-yb-build`,
so that it does not conflict with any other Linuxbrew installation on your workstation, and does
not contain any unnecessary packages that would interfere with the build.

```
git clone git@github.com:linuxbrew/brew.git ~/.linuxbrew-yb-build
~/.linuxbrew-yb-build/bin/brew install autoconf automake boost flex gcc libtool
```

You don't need to add `~/.linuxbrew-yb-build/bin` to PATH. The build scripts will automatically
discover this Linuxbrew installation.

### Mac OS X

Install [Homebrew](https://brew.sh/):

```bash
/usr/bin/ruby -e "$(
  curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```

Install the following packages using Homebrew:
```
brew install autoconf automake bash bison boost ccache cmake coreutils flex gnu-tar libtool \
             pkg-config pstree wget zlib
```

Also YugaByte build scripts rely on Bash 4. Make sure that `which bash` outputs
`/usr/local/bin/bash` before proceeding. You may need to put `/usr/local/bin` as the first directory
on PATH in your `~/.bashrc` to achieve that.

### All platforms: Java prerequisites

YugaByte core is written in C++, but the repository contains Java code needed to run sample
applications. To build the Java part, you need:
* JDK 8
* [Apache Maven](https://maven.apache.org/).

Also make sure Maven's `bin` directory is added to your PATH, e.g. by adding to your `~/.bashrc`
```
export PATH=$HOME/tools/apache-maven-3.5.0/bin:$PATH
```
if you've installed Maven into `~/tools/apache-maven-3.5.0`.

For building YugaByte Java code, you'll need to install Java and Apache Maven.

### Java Driver

YugaByte and Apache Cassandra use different approaches to split data between nodes. In order to
route client requests to the right server without extra hops, we provide a [custom
LoadBalancingPolicy](https://goo.gl/At7kvu) in [our modified version
](https://github.com/yugabyte/cassandra-java-driver) of Datastax's Apache Cassandra Java driver.

The latest version of our driver is available on Maven Central. You can build your application
using our driver by adding the following Maven dependency to your application:

```
<dependency>
  <groupId>com.yugabyte</groupId>
  <artifactId>cassandra-driver-core</artifactId>
  <version>3.2.0-yb-8</version>
</dependency>
```

## Building YugaByte code

Assuming this repository is checked out in `~/code/yugabyte-db`, do the following:

```
cd ~/code/yugabyte-db
./yb_build.sh release --with-assembly
```

The above command will build the release configuration, put the C++ binaries in
`build/release-gcc-dynamic-community`, and will also create the `build/latest` symlink to that
directory. Then it will build the Java code as well. The `--with-assembly` flag tells the build
script to build the `yb-sample-apps.jar` file containing sample Java apps.

## Running a Local Cluster

Now you can follow the [Communty Edition Quick Start / Create local cluster
](http://docs.yugabyte.com/community-edition/quick-start/#create-local-cluster) tutorial
to create a local cluster and test it using Apache CQL shell and Redis clients, as well as run
the provied Java sample apps.

## Comparison with other Databases
Please review https://docs.yugabyte.com/architecture/comparisons/
to see how we compare with other SQL and NoSql databases in the market.
* Apache Cassandra https://docs.yugabyte.com/architecture/comparisons/cassandra/
* Redis https://docs.yugabyte.com/architecture/comparisons/redis/
* Apache HBase https://docs.yugabyte.com/architecture/comparisons/hbase/

## Reporting Issues

Please use [GitHub issues](https://github.com/YugaByte/yugabyte-db/issues) to report issues.
Also feel free to post on the [YugaByte Community Forum](http://forum.yugabyte.com).

## Contributing

We accept contributions as GitHub pull requests. Our code style is available
[here](https://goo.gl/Hkt5BU)
(mostly based on [Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html)).

## License

YugaByte Community Edition is distributed under an Apache 2.0 license. See the
[LICENSE.txt](https://github.com/YugaByte/yugabyte-db/blob/master/LICENSE.txt) file for
details.
