# spark-utils
[![](https://jitpack.io/v/whaitukay/spark-utils.svg)](https://jitpack.io/#whaitukay/spark-utils)

This is a collection of uncommon functions that are commonly used in projects.  
The aim of the library is to reduce boilerplate code across projects.

## Features
The spark-utils library currently contains the following util functions:

* writeMergedCsv - A function for saving a DataFrame to a single CSV file.
* listFiles - A function to list all the child files for a given path.
* zipFile - A function that will zip (deflate) a given file.
* binaryJoin - A function that joins dataframes together on a single key in a much more efficient manner.

## Installation

### Spark
The library can be added as a dependency using spark config
```shell script
./spark-submit --master ... --conf spark.jars.packages="com.github.whaitukay:spark-utils:0.2.6" --conf spark.jars.repositories="https://jitpack.io" ...
```

### SBT
To add the library to your SBT project, add the following content to your `build.sbt` file.
```scala
resolvers += "jitpack" at "https://jitpack.io"
libraryDependencies += "com.github.whaitukay" % "spark-utils" % "0.2.6"	
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

### delete
The `delete` function is used to delete a file or directory in a given path.

It can be used as follows in a `%spark` and `%pyspark` paragraph

```scala
% spark

import com.github.whaitukay.utils.Util

val path = "/path/to/file_or_directory" //some path to a file or directory

// Delete the file or directory at the specified path
val delFileOrDir = Util.delete(path)
```

```python
%pyspark
import spark_utils

path = "/path/to/file_or_directoryt" #some path to a file or directory

# Delete the file or directory at the specified path
delFileOrDir = spark_utils.delete(path)
```

Note that the `delete` function returns a `Boolean` value if it was a success or an error if the path did not exist.

### rename
The `rename` function is used to rename a specific file in a given path.

It can be used as follows in a `%spark` and `%pyspark` paragraph

```scala
%spark
import com.github.whaitukay.utils.Util

val inputPath = "/path/to/some/file"  //path to some file to be renamed
val outputPath = "/path/to/some/renamedfile" //renamed path of the file

// Renames a file in the specified path
val renameFile = Util.rename(inputPath, outputPath)
```

```python
%pyspark
import spark_utils

inputPath = "/path/to/some/file"  #path to some file to be renamed
outputPath = "/path/to/some/renamedfile" #renamed path of the file

# Renames a file in the specified path
renameFile = spark_utils.rename(inputPath, outputPath)
```

Note that the `rename` function returns a `Boolean` value.

### copyMove
The `copyMove` function is used to copy or move files in directory to another location in hadoop.

It can be used as follows in a `%spark` and `%pyspark` paragraph

```scala
% spark

import com.github.whaitukay.utils.Util

val inputPath = "/path/to/some/directory" //path to some files to be moved
val outputPath = "/path/to/some/new/directory" //path to the new file location

// Move files from one directory to another, keeping the original files
val moveFiles = Util.copyMove(inputPath, outputPath)

// Move files from one directory to another and deleting the original files
val moveFiles = Util.copyMove(inputPath, outputPath, deleteSrc = true)
```

```python
%pyspark
import spark_utils

inputPath = "/path/to/some/directory"  #path to some files to be moved
outputPath = "/path/to/some/new/directory" #path to the new file location

# Move files from one directory to another, keeping the original files
moveFiles = spark_utils.copyMove(inputPath, outputPath)

# Move files from one directory to another and deleting the original files
moveFiles = spark_utils.copyMove(inputPath, outputPath, deleteSrc = True)
```

Note that the `copyMove` function returns a `Boolean` value.

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

### binaryJoin

```scala
%spark
import com.github.whaitukay.utils.Util

val dfSeq = Seq(df1, df2, ..., dfn) // A collection of dataframes

// Joins all the dataframes together on the key "id" using a "left-join" approach
val joinedDF = Util.binaryJoin(dfSeq, "id", "left")
```

```python
%pyspark
import spark_utils

df_list = list([df1, df2, ..., dfn])  # A collection of dataframes

# Joins all the dataframes together on the key "id" using a "left-join" approach
joined_df = spark_utils.binaryJoin(df_list, 'id', 'left')
```

## Future Work
Some work that is currently planned to be added includes:

* zipping multiple files (or a directory)
* snapshotting utils

As more users start using the platform, it will be very likely that more user-created content will end up being copy-pasted all over different notes.
Once these snippets have been identified, it can be added to this library to make it simpler for other users to use it in their code aswell.

