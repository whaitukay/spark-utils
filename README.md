# spark-utils
[![](https://jitpack.io/v/whaitukay/spark-utils.svg)](https://jitpack.io/#whaitukay/spark-utils)

I have create a spark-utils library on github that contains some commonly used functions.
This will allow the devs to only have to call the library to use these functions, instead of having to add the boilerplate code everytime.
Fixes or enhancements made to these functions can then also be captured in the library and propagated to all notes using it.

## Features
The spark-utils library currently contains the following util functions:

* writeMergedCsv - A function for saving a DataFrame to a single CSV file.
* listFiles - A function to list all the child files for a given path.
* zipFile - A function that will zip (deflate) a given file.

## Installation

### Spark
The library can be added as a dependency using spark config
```shell script
./spark-submit --master ... --conf spark.jars.packages="com.github.whaitukay:spark-utils:0.1.1" --conf spark.jars.repositories="https://jitpack.io" ...
```

### SBT
To add the library to your SBT project, add the following content to your `build.sbt` file.
```scala
resolvers += "jitpack" at "https://jitpack.io"
libraryDependencies += "com.github.whaitukay" % "spark-utils" % "0.1.1"	
```

### Zeppelin Notebook
For now, this library needs to be added as a dependency at the start of the note (before starting a session using the `%spark.dep` interpreter)

```scala
%spark.dep
z.reset()

z.addRepo("jitty").url("https://jitpack.io")
z.load("com.github.whaitukay:spark-utils:0.1.1")
```

## Usage
Brief instructions on how to use the library

### writeMergedCsv
The `writeMergedCsv` function is used to write a DataFrame to a single CSV file.
It can be used as follows in a `%spark` paragraph

```scala
%spark
import com.github.whaitukay.utils.Util

val df = ... //some dataframe that you have

// Write DataFrame as a csv using defaults (comma-separated and overwrite)
Util.writeMergedCsv(df, "s3a://mdp-analytics-data/some/output/path.csv")

// Write DataFrame as a csv with a custom delimiter
Util.writeMergedCsv(df, "s3a://mdp-analytics-data/some/output/path.csv", delimiter = "|")

// Write DataFrame as a csv without overwrite (will fail if the file exists)
Util.writeMergedCsv(df, "s3a://mdp-analytics-data/some/output/path.csv", overwrite = false)

// Write DataFrame as a csv with custom delimiter and without overwrite (will fail if the file exists)
Util.writeMergedCsv(df, "s3a://mdp-analytics-data/some/output/path.csv", delimiter = "~", overwrite = false)
```

### listFiles
The `listFiles` function is used to list all the files (or directories) in a given path.
This can be useful when wanting to read multiple files programatically.

It can be used as follows in a `%spark` paragraph

```scala
%spark
import com.github.whaitukay.utils.Util

val path = "s3a://mdp-data-materialized/xds" //some path to list

// List all the files (or directories) in the specified path
val files = Util.listFiles(path)
```

Note that the `listFiles` function returns a `Seq[String]` value.

### zipFile
The `zipFile` function is used to add the file to a zip (deflate) archive.
This can be useful when trying to save on bandwidth when shifting files from MDP to local.

It can be used as follows in a `%spark` paragraph

```scala
%spark
import com.github.whaitukay.utils.Util

val inputPath = "s3a://mdp-analytics-data/samples/iris.csv"  //path to some file to be zipped
val outputPath = "s3a://mdp-analytics-data/samples/iris.zip" //target path to save the zipped file

// Attempt to zip the file foudn at the inputPath, and save the zipped file at the outputPath
Util.zipFile(inputPath, outputPath)
```

Note that the `zipFile` function will automatically add the ".zip" extention if it is not present in the outputPath.

## Notes to pySpark users
Unfortunately my python-fu is not strong enough to create wrappers for you guys just yet.
Luckily, since we are using the notebook, we can switch bewteen `%pyspark` and `%spark` and still use the same session.

This means that if you have created a view using `df.createOrReplaceTempView('my_pyspark_df')`, you can recall it using `spark.table("my_pyspark_df")`.

So as a work-around, you can do everything you need to do in pySpark, and when you require the utils, save the DataFrame as a temp view, and switch to scala spark.

```python
%pyspark

df = ... # some python df that you might have

df.createOrReplaceTempView('my_df')
```

```scala
%spark
import com.github.whaitukay.utils.Util

val df = spark.table("my_df")

// Write DataFrame as a csv using defaults (comma-separated and overwrite)
Util.writeMergedCsv(df, "s3a://mdp-analytics-data/some/output/path.csv")
```

If any of you pythonistas would like to assist or contribute to creating wrappers, you are welcome to do so.

## Future Work
Some work that is currently planned to be added includes:

* zipping multiple files (or a directory)
* snapshotting utils

As more users start using the platform, it will be very likely that more user-created content will end up being copy-pasted all over different notes.
Once these snippets have been identified, it can be added to this library to make it simpler for other users to use it in their code aswell.

