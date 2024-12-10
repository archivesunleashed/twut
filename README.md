# Tweet Archives Unleashed Toolkit (twut)

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.archivesunleashed/twut/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.archivesunleashed/twut)
[![LICENSE](https://img.shields.io/badge/license-Apache-blue.svg?style=flat)](https://www.apache.org/licenses/LICENSE-2.0)
[![Contribution Guidelines](http://img.shields.io/badge/CONTRIBUTING-Guidelines-blue.svg)](./CONTRIBUTING.md)

An open-source toolkit for analyzing line-oriented JSON data from the Twitter v1.1 API using Apache Spark.

## Dependencies

- Java 8 or 11
- Python 3
- [Apache Spark](https://spark.apache.org/downloads.html)

## Getting Started

To get started with `twut`, you can either use it directly from Maven or download the JAR and ZIP files for Spark or PySpark.

### Using the Spark Shell

To use `twut` with Apache Spark, you can use the following command to include the package:

```
$ spark-shell --packages "io.archivesunleashed:twut:1.0.0"
```

Alternatively, you can download the JAR file from the [latest release](https://github.com/archivesunleashed/twut/releases) and include it manually:

```
$ spark-shell --jars /path/to/twut-1.0.0-fatjar.jar
```

### Using PySpark

For Python users, download the ZIP file from the [latest release](https://github.com/archivesunleashed/twut/releases) and include it in your PySpark environment:

```
$ pyspark --py-files /path/to/twut-1.0.0.zip
```

You will also need to set the `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` environment variables.

## Documentation and Tutorials

After you have `twut` built or downloaded, you can follow the basic set of recipes and tutorials [here](https://github.com/archivesunleashed/twut/tree/main/docs/usage.md).

## License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

## Acknowledgments

This work is primarily supported by the [Andrew W. Mellon Foundation](https://mellon.org/). Other financial and in-kind support comes from the [Social Sciences and Humanities Research Council](http://www.sshrc-crsh.gc.ca/), [Compute Canada](https://www.computecanada.ca/), the [Ontario Ministry of Research, Innovation, and Science](https://www.ontario.ca/page/ministry-research-innovation-and-science), [York University Libraries](https://www.library.yorku.ca/web/), [Start Smart Labs](http://www.startsmartlabs.com/), and the [Faculty of Arts](https://uwaterloo.ca/arts/) and [David R. Cheriton School of Computer Science](https://cs.uwaterloo.ca/) at the [University of Waterloo](https://uwaterloo.ca/).

Any opinions, findings, and conclusions or recommendations expressed are those of the researchers and do not necessarily reflect the views of the sponsors.
