# spark-utils
[![](https://jitpack.io/v/whaitukay/spark-utils.svg)](https://jitpack.io/#whaitukay/spark-utils)

This is a collection of uncommon functions that are commonly used in projects.  
The aim of the library is to reduce boilerplate code across projects.

## Features
The spark-utils library currently contains the following util functions:

* writeMergedCsv - A function for saving a DataFrame to a single CSV file.
* listFiles - A function to list all the child files for a given path.
* zipFile - A function that will zip (deflate) a given file.

## Installation

### Spark
The library can be added as a dependency using spark config
```shell script
./spark-submit --master ... --conf spark.jars.packages="com.github.whaitukay:spark-utils:0.2.2" --conf spark.jars.repositories="https://jitpack.io" ...
```

### SBT
To add the library to your SBT project, add the following content to your `build.sbt` file.
```scala
resolvers += "jitpack" at "https://jitpack.io"
libraryDependencies += "com.github.whaitukay" % "spark-utils" % "0.2.2"	
```

### Zeppelin Notebook
For now, this library needs to be added as a dependency at the start of the note (before starting a session using the `%spark.dep` interpreter)

```scala
%spark.dep
z.reset()

z.addRepo("jitpack").url("https://jitpack.io")
z.load("com.github.whaitukay:spark-utils:0.2.2")
```

## Usage
Brief instructions on how to use the library

### writeMergedCsv
The `writeMergedCsv` function is used to write a DataFrame to a single CSV file.
It can be used as follows in a `%spark` and `%pyspark` paragraph

```scala
%spark
import com.github.whaitukay.utils.Util

val df = ... //some dataframe that you have

// Write DataFrame as a csv using defaults (comma-separated and overwrite)
Util.writeMergedCsv(df, "/path/to/some/output/file")

// Write DataFrame as a csv with a custom delimiter
Util.writeMergedCsv(df, "/path/to/some/output/file", delimiter = "|")

// Write DataFrame as a csv without overwrite (will fail if the file exists)
Util.writeMergedCsv(df, "/path/to/some/output/file", overwrite = false)

// Write DataFrame as a csv with custom delimiter and without overwrite (will fail if the file exists)
Util.writeMergedCsv(df, "/path/to/some/output/file", delimiter = "~", overwrite = false)
```

```python
%pyspark
import spark_utils

df = ... #some dataframe that you have

# Write DataFrame as a csv using defaults (comma-separated and overwrite)
spark_utils.writeMergedCsv(df, "/path/to/some/output/file")

# Write DataFrame as a csv with a custom delimiter
spark_utils.writeMergedCsv(df, "/path/to/some/output/file", delimiter = "|")

# Write DataFrame as a csv without overwrite (will fail if the file exists)
spark_utils.writeMergedCsv(df, "/path/to/some/output/file", overwrite = False)

# Write DataFrame as a csv with custom delimiter and without overwrite (will fail if the file exists)
spark_utils.writeMergedCsv(df, "/path/to/some/output/file", delimiter = "~", overwrite = False)
```

### listFiles
The `listFiles` function is used to list all the files (or directories) in a given path.
This can be useful when wanting to read multiple files programatically.

It can be used as follows in a `%spark` and `%pyspark` paragraph

```scala
%spark
import com.github.whaitukay.utils.Util

val path = "/path/to/list" //some path to list

// List all the files (or directories) in the specified path
val files = Util.listFiles(path)
```

```python
%pyspark
import spark_utils

path = "/path/to/list" #some path to list

# List all the files (or directories) in the specified path
files = spark_utils.listFiles(path)
```

Note that the `listFiles` function returns a `Seq[String]` value.

### zipFile
The `zipFile` function is used to add the file to a zip (deflate) archive.
This can be useful when trying to save on bandwidth when shifting files from MDP to local.

It can be used as follows in a `%spark` and `%pyspark` paragraph

```scala
%spark
import com.github.whaitukay.utils.Util

val inputPath = "/path/to/some/input/file"  //path to some file to be zipped
val outputPath = "/path/to/some/output/file" //target path to save the zipped file

// Attempt to zip the file found at the inputPath, and save the zipped file at the outputPath
Util.zipFile(inputPath, outputPath)

import com.github.whaitukay.utils.zipper.ZipUtil
// Attempt to zip the file found at the inputPath, and save the zipped file at the outputPath through a specific temporary directory
ZipUtil.zipFile(inputPath, outputPath, "/tmp")
```

```python
%pyspark
import spark_utils

inputPath = "/path/to/some/input/file"  #path to some file to be zipped
outputPath = "/path/to/some/output/file" #target path to save the zipped file

# Attempt to zip the file found at the inputPath, and save the zipped file at the outputPath
spark_utils.zipFile(inputPath, outputPath)

# Attempt to zip the file found at the inputPath, and save the zipped file at the outputPath through a specific temporary directory
spark_utils.zipFile(inputPath, outputPath, "/tmp")
```

Note that the `zipFile` function will automatically add the ".zip" extension if it is not present in the outputPath.

## Future Work
Some work that is currently planned to be added includes:

* zipping multiple files (or a directory)
* snapshotting utils

As more users start using the platform, it will be very likely that more user-created content will end up being copy-pasted all over different notes.
Once these snippets have been identified, it can be added to this library to make it simpler for other users to use it in their code aswell.

