# Tweet Archives Unleashed Toolkit (twut)

[![Build Status](https://travis-ci.org/archivesunleashed/twut.svg?branch=master)](https://travis-ci.org/archivesunleashed/twut)
[![codecov](https://codecov.io/gh/archivesunleashed/twut/branch/master/graph/badge.svg)](https://codecov.io/gh/archivesunleashed/twut)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.archivesunleashed/twut/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.archivesunleashed/twut)
[![Scaladoc](https://javadoc-badge.appspot.com/io.archivesunleashed/twut.svg?label=scaladoc)](http://api.docs.archivesunleashed.io/0.18.0/scaladocs/index.html)
[![LICENSE](https://img.shields.io/badge/license-Apache-blue.svg?style=flat)](https://www.apache.org/licenses/LICENSE-2.0)
[![Contribution Guidelines](http://img.shields.io/badge/CONTRIBUTING-Guidelines-blue.svg)](./CONTRIBUTING.md)

An open-source toolkit for analyzing line-oriented JSON Twitter archives with Apache Spark.

## Getting Started

### Easy

If you have Apache Spark ready to go, it's as easy as:

```
$ spark-shell --packages "io.archivesunleashed:twut:0.0.1-SNAPSHOT"
```

### A little less easy

You can download the [latest release here](https://github.com/archivesunleashed/twut/releases) and include it like so:

```
$ spark-shell --jars /path/to/twut-0.0.1-SNAPSHOT-fatjar.jar"
```

## Usage

`twut` expects Tweets to be supplied in a DataFrame.

Example:

```
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.0.0-preview
      /_/
         
Using Scala version 2.12.10 (OpenJDK 64-Bit Server VM, Java 1.8.0_222)
Type in expressions to have them evaluated.
Type :help for more information.

scala> import io.archivesunleashed._
import io.archivesunleashed._

scala> val tweets = "/home/nruest/Projects/au/twut/src/test/resources/10-sample.jsonl"
tweets: String = /home/nruest/Projects/au/twut/src/test/resources/10-sample.jsonl

scala> val tweetsDF = spark.read.json(tweets)
19/12/02 13:38:51 WARN package: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.sql.debug.maxToStringFields'.
tweetsDF: org.apache.spark.sql.DataFrame = [contributors: string, coordinates: string ... 33 more fields]

scala> ids(tweetsDF).show
+-------------------+
|             id_str|
+-------------------+
|1201505319257403392|
|1201505319282565121|
|1201505319257608197|
|1201505319261655041|
|1201505319261597696|
|1201505319274332165|
|1201505319261745152|
|1201505319270146049|
|1201505319286755328|
|1201505319286984705|
+-------------------+
```

## Documentation! Or, how do I use this?

Once built or downloaded, you can follow the basic set of recipes and tutorials [here](https://github.com/archivesunleashed/twut/wiki/).

# License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

# Acknowledgments

This work is primarily supported by the [Andrew W. Mellon Foundation](https://mellon.org/). Other financial and in-kind support comes from the [Social Sciences and Humanities Research Council](http://www.sshrc-crsh.gc.ca/), [Compute Canada](https://www.computecanada.ca/), the [Ontario Ministry of Research, Innovation, and Science](https://www.ontario.ca/page/ministry-research-innovation-and-science), [York University Libraries](https://www.library.yorku.ca/web/), [Start Smart Labs](http://www.startsmartlabs.com/), and the [Faculty of Arts](https://uwaterloo.ca/arts/) and [David R. Cheriton School of Computer Science](https://cs.uwaterloo.ca/) at the [University of Waterloo](https://uwaterloo.ca/).

Any opinions, findings, and conclusions or recommendations expressed are those of the researchers and do not necessarily reflect the views of the sponsors.
