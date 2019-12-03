# Tweet Archives Unleashed Toolkit (twut)

[![Build Status](https://travis-ci.org/archivesunleashed/twut.svg?branch=master)](https://travis-ci.org/archivesunleashed/twut)
[![codecov](https://codecov.io/gh/archivesunleashed/twut/branch/master/graph/badge.svg)](https://codecov.io/gh/archivesunleashed/twut)
[![LICENSE](https://img.shields.io/badge/license-Apache-blue.svg?style=flat)](https://www.apache.org/licenses/LICENSE-2.0)
[![Contribution Guidelines](http://img.shields.io/badge/CONTRIBUTING-Guidelines-blue.svg)](./CONTRIBUTING.md)

An open-source toolkit for analyzing line-oriented JSON Twitter archives with Apache Spark.

## Dependencies

- Java 8 or 11
- [Apache Spark](https://spark.apache.org/downloads.html)

## Getting Started

Until we have a release, you'll need to clone the repo, build it, and pass the jar to Apache Spark.

```shell

$ git clone https://github.com/archivesunleashed/twut.git
$ cd twut
$ mvn clean install
$ /path/to/spark/bin/spark-shell --jars /path/to/twut-0.0.1-SNAPSHOT-fatjar.jar"

Spark context Web UI available at http://10.0.1.44:4040
Spark context available as 'sc' (master = local[*], app id = local-1575383157031).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.0.0-preview
      /_/

Using Scala version 2.12.10 (OpenJDK 64-Bit Server VM, Java 1.8.0_222)
Type in expressions to have them evaluated.
Type :help for more information.

scala>
```

## Documentation! Or, how do I use this?

Once built or downloaded, you can follow the basic set of recipes and tutorials [here](https://github.com/archivesunleashed/twut/wiki/).

# License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

# Acknowledgments

This work is primarily supported by the [Andrew W. Mellon Foundation](https://mellon.org/). Other financial and in-kind support comes from the [Social Sciences and Humanities Research Council](http://www.sshrc-crsh.gc.ca/), [Compute Canada](https://www.computecanada.ca/), the [Ontario Ministry of Research, Innovation, and Science](https://www.ontario.ca/page/ministry-research-innovation-and-science), [York University Libraries](https://www.library.yorku.ca/web/), [Start Smart Labs](http://www.startsmartlabs.com/), and the [Faculty of Arts](https://uwaterloo.ca/arts/) and [David R. Cheriton School of Computer Science](https://cs.uwaterloo.ca/) at the [University of Waterloo](https://uwaterloo.ca/).

Any opinions, findings, and conclusions or recommendations expressed are those of the researchers and do not necessarily reflect the views of the sponsors.
