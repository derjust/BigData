<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->


- [Going Big](#going-big)
  - [Processing Big Data with Bigtable, Dataflow and BigQuery](#processing-big-data-with-bigtable-dataflow-and-bigquery)
  - [The flow](#the-flow)
  - [Basic setup](#basic-setup)
  - [Brining in the big guns](#brining-in-the-big-guns)
  - [Pipeline skeleton](#pipeline-skeleton)
  - [Reading the weather data](#reading-the-weather-data)
  - [Side input to resolve geo data](#side-input-to-resolve-geo-data)
  - [Lookup Logic](#lookup-logic)
  - [Filtering and more Aggregating Logic](#filtering-and-more-aggregating-logic)
  - [Output Logic 1 - Only Aggregators](#output-logic-1---only-aggregators)
  - [Output Logic 2 - Side outputs](#output-logic-2---side-outputs)
  - [Write Logic 1 - Bigtable](#write-logic-1---bigtable)
  - [Write Logic 2 - BigQuery](#write-logic-2---bigquery)
  - [Testing the Pipes](#testing-the-pipes)
  - [Running it](#running-it)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Going Big
## Processing Big Data with Bigtable, Dataflow and BigQuery

With the release of Bigtable, Google has enriched its tools suite to process data on a massive scale.

Combining these technologies opens the door to answering complex questions more easily and cheaply than ever before - e.g., [Scaling to Build the Consolidated Audit Trail](https://cloud.google.com/bigtable/pdf/ConsolidatedAuditTrail.pdf).

![Pipeline](https://raw.githubusercontent.com/derjust/BigData/master/doc/dataflow.png)

Using publicly-available datasets, this article will focus on the combination of these technologies.  
While this article has a more specific focus, more details and a general coverage of the technologies can be found here:

* [Dataflow](https://cloud.google.com/dataflow/what-is-google-cloud-dataflow)
* [BigQuery](https://cloud.google.com/bigquery/what-is-bigquery)
* [Bigtable](https://cloud.google.com/bigtable/docs/)

This article will refer to specific lessons learned while working with these technologies, which will be available for reference in the "**Hint**" sections. 

## The flow

For demonstrative purposes, the following tasks will be performed:

* Take the 114 billion rows of NOAA [weather data](https://cloud.google.com/bigquery/sample-tables?hl=en)...
 * Reading from BigQuery into the Dataflow
* Combine them with [static geo data](ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-history.txt) for further analysis...
 * Using Dataflow's [side input](https://cloud.google.com/dataflow/model/par-do#side-inputs) capabilities
* Filter and aggreage the temperature at a given location...
 * Using Dataflow's [Filter](https://cloud.google.com/dataflow/model/transforms) and [Aggregators](https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/transforms/Aggregator).
* And, finally, writing to Bigtable and BigQuery for further geo-based analysis
 * Using Bigtable and BigQuery API

## Basic setup
Infrastructurally, setting up a Google Cloud Platform account is all it takes.  
For the developer, an updated Java development environment is obviously required:

* Java 7 or higher
* Maven 3 - it is left as an exercise for the reader to use Gradle

### Preparing Cloud services

In order to execute the Dataflow pipeline we are going to build, we will need some minor setup via Google's Cloud Console:

1. Create a Cloud Storage Bucket `noaa` and upload file this file to it: [ftp.ncdc.noaa.gov/pub/data/noaa/isd-history.csv](ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-history.csv).
1. Create a Bigtable cluster `noaa-cruncher` - The minimum of 3 nodes is sufficient

Also, set up the [HBase shell](https://cloud.google.com/bigtable/docs/hbase-shell-quickstart) and create a table via:

```
> create 'weather', 'location', 'weather'
```

This creates a new table with the column families `location` and `weather`. Details about structuring these tables can be [found here](https://cloud.google.com/bigtable/docs/schema-design).

Finally create a BigQuery dataset `weather` in the [BigQuery UI](https://bigquery.cloud.google.com/).

The rest of the article will outline all the required files. A basic understanding of Java and Maven is assumed.

The whole project is also available for reference at [github.com/SunGard-Labs/Dataflow-Blogpost](https://github.com/derjust/BigData)

## Bringing in the big guns
Starting with a classic `pom.xml` and a Maven-style project setup is all it takes to bring the Google technologies together:

The libraries for Dataflow and Bigtable continue to undergo active development. We are going to use the LATEST version in order to have at our disposal the latest improvements. The snapshot releases can be found at this repository: [https://oss.sonatype.org/content/repositories/snapshots](https://oss.sonatype.org/content/repositories/snapshots). At the time of writing this is `0.2.2-SNAPSHOT`

```xml
<properties>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <maven.compiler.source>1.7</maven.compiler.source>
    <maven.compiler.target>1.7</maven.compiler.target>
</properties>

<dependency>
	<groupId>com.google.cloud.bigtable</groupId>
	<artifactId>bigtable-hbase-dataflow</artifactId>
	<version>LATEST</version>
</dependency>
<dependency>
	<groupId>org.slf4j</groupId>
	<artifactId>slf4j-api</artifactId>
	<version>1.7.7</version>
	<scope>compile</scope>
</dependency>
```

**Hint:** Dataflow uses [Cloud Logging](https://cloud.google.com/logging/docs/) to log all events. It is highly recommended to use this infrastructure also for the logging of the processing logic itself.  
To do so, slf4j must be used (alternative: `commons-logging` is also fine). But it must be ensured that **neither** `log4j` is available on the classpath (that would confuse the Bigtable logging, as it uses `commons-logging`), **nor** any `slf4j-log4j12` (or other) binding is available for `slf4j`. `slf4j-api` is all that is required in your `pom.xml`. The rest is performed/referenced as a transitive dependency via the Dataflow dependency.  
Just double-check your (transitive) dependencies with `mvn dependency:tree` (or your IDE).

## Pipeline skeleton

A Dataflow pipeline starts as a simple java program. Luckily, argument parsing comes built-in with the Google libraries:

```java
package com.sungard.advtech;

import java.util.Map;

import org.apache.hadoop.hbase.client.Mutation;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigtable.dataflow.CloudBigtableIO;
import com.google.cloud.bigtable.dataflow.CloudBigtableOptions;
import com.google.cloud.bigtable.dataflow.CloudBigtableTableConfiguration;
import com.google.cloud.bigtable.dataflow.HBaseMutationCoder;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.TableRowJsonCoder;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineWorkerPoolOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.Filter;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TupleTagList;
import com.sungard.advtech.data.StationDetails;
import com.sungard.advtech.data.WeatherAtStation;

public class CloudyWithMeatballs {
    /** Groups together the argument parsing for the Bigtable settings and the Dataflow worker details */
    public static interface Options extends CloudBigtableOptions, DataflowPipelineWorkerPoolOptions {
        /** Defines the BigQuery table to write the output to*/
        String getBigQueryTable();
        void setBigQueryTable(String table);

        /** Defines the CSV file to resolve the weather station data from */
        String getIsdHistory();
        void setIsdHistory(String history);
    }

    public static void main(String[] args) {
        // Parses and validates the command line arguments
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        // Create the Dataflow pipeline based on the provided arguments
        Pipeline p = Pipeline.create(options);
        // Initialize Bigtable on top of the Dataflow pipeline
        p = CloudBigtableIO.initializeForWrite(p);

        /* Pipeline processing code will go here. Replace with following code */

        p.run();
    }
}
```

## Reading the weather data

Reading the weather data is simple - Google provides the data set as part of their [sample data](https://bigquery.cloud.google.com/table/publicdata:samples.gsod).
Use the SQL console to verify that queries return the intended result. Go there and test this query:

```sql
SELECT wban_number, station_number, year, month, day, mean_temp, min_temperature, max_temperature
FROM publicdata:samples.gsod
WHERE year = 2010
LIMIT 1000
```

The Dataflow code is as simple as

```java
// Read the main PColleciton from BigQuery
PCollection<TableRow> weatherData = p.apply(
    BigQueryIO.Read.named("Read Weather Data")
      .fromQuery("SELECT wban_number, station_number, year, month, day, mean_temp, min_temperature, max_temperature"
      + " FROM publicdata:samples.gsod"));
```

This will execute the query and return each row as an input element into the pipeline.  
For simplicity and performance reasons we will not engage in any fancy object mapping, but will operate on the Google datatypes right away.  
For BigQuery's `TableRow`, that means we have a map-style data structure, in which each column name is the key of that map.


## Side input to resolve geo data
The basic concept of Dataflow is to have a single, main `PCollection` of elements flowing through the pipeline. Whereas that is sufficient for a broad range of situations, sometimes it is required to assemble data from various sources or output two streams of data.
For [this kind of sitaution](https://cloud.google.com/dataflow/pipelines/design-principles), Dataflow provides [side input/outputs](https://cloud.google.com/dataflow/model/par-do#side-inputs).

In order to resolve the weather data from above to a location on planet earth, read the (previously uploaded) station details from the Cloud Storage and use this class to store the details from the CSV file:

```java
package com.sungard.advtech.data;

public class StationDetails implements java.io.Serializable {
    private static final long serialVersionUID = 1L;
    float lat;
    float lon;
    String name;
    public float getLat() {
        return lat;
    }
    public void setLat(float lat) {
        this.lat = lat;
    }
    public float getLon() {
        return lon;
    }
    public void setLon(float lon) {
        this.lon = lon;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public static StationDetails of(String name, float lat, float lon) {
        StationDetails sd = new StationDetails();
        sd.name = name;
        sd.lat = lat;
        sd.lon = lon;
        return sd;
    }
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Float.floatToIntBits(lat);
        result = prime * result + Float.floatToIntBits(lon);
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        return result;
    }
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        StationDetails other = (StationDetails) obj;
        if (Float.floatToIntBits(lat) != Float.floatToIntBits(other.lat))
            return false;
        if (Float.floatToIntBits(lon) != Float.floatToIntBits(other.lon))
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        return true;
    }
    @Override
    public String toString() {
        return String.format("%s: %+09.4f|%+09.4f", name, lat, lon);
    }
}
```

and parse the file with a "highly optimistic" CSV parsing approach:

```java
package com.sungard.advtech;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.sungard.advtech.data.StationDetails;

/**
 * Very optimistic parser to load the ISD history CSV file.
 * //TODO sophisticated error checking
 */
public class ISDHistoryParser extends DoFn<String, KV<String, StationDetails>> {
    private static final long serialVersionUID = 1L;
    private static final String headerLineBeginning = "\"USAF";
    private static final Logger LOG = LoggerFactory.getLogger(ISDHistoryParser.class);

    @Override
    public void processElement(ProcessContext context) {
        // Get a new line from the CSV file
        String csvLine = context.element();

        // Skip the CSV header line
        if (csvLine.startsWith(headerLineBeginning)) {
            return;
        }

        // Get access to the fields of the row record
        String[] csvFields = csvLine.split(",");
        if (csvFields.length != 11) {
            LOG.warn("Could not parse '{}'", csvLine);
            return;
        }

        // Extract the interesting fields from the row record
        String stationNumber = stripQuotes(csvFields[0]);
        String stationName = stripQuotes(csvFields[2]);
        try {
            if ("\"\"".equals(csvFields[6]) || "\"\"".equals(csvFields[7])) {
                LOG.warn("Could not parse '{}'", csvLine);
                return;
            }
            float lat = parseFloat(csvFields[6]);
            float lon = parseFloat(csvFields[7]);

            context.output(KV.of(stationNumber, StationDetails.of(stationName, lat, lon)));
        } catch (NumberFormatException|StringIndexOutOfBoundsException e) {
            LOG.warn(String.format("Skipping line '%s' due to unparsable: %s", csvLine, e.getMessage()), e);
        }
    }

    private float parseFloat(String input) {
        String number = stripQuotes(input);
        return Float.parseFloat(number);
    }

    /** Removes the first and last character (quotes) which are around every CSV field. */
    private String stripQuotes(String input) {
        return input.substring(1, input.length() - 1);
    }
}
```

and add this to the pipeline:

```java
// Prepare the side input...
PCollection<KV<String, StationDetails>> isdHistory = p
    .apply(TextIO.Read.named("Read ISD History").from(options.getIsdHistory()))
    .apply(ParDo.named("Parse ISD History").of(new ISDHistoryParser()));
```

**Hint:** If an `Exception` is thrown from within `DoFn.processElement(ProcessContext)`, Dataflow retries the offending element on a different worker. This is nice for connectivity issues to other services and so forth.
But uncorrectable `Exceptions` (such as, malformed data) will always fail.
If too many elements fail, the whole pipeline is stopped.

**Hint:** Cloud Logging preserves the stacktrace as long as the appropriate method on `Logger` is used. Causing excessive logging bursts (i.e. due to logging each stack element on its own) should be avoided.

## Lookup Logic
To use the `PCollection` as side input, it has to be mapped to a view to ensure immutability. As the ISD History contains duplicate items for the same station number, a simple "High Lander"- logic is implemented to resolve to a unique map item [as required by the Dataflow API](https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/transforms/View.AsMap):

```java
package com.sungard.advtech;

import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.sungard.advtech.data.StationDetails;

/**
 * This high lander (there can be only one) function operates on a 'last object takes it all'.
 * As it is executed in a distributed environment, 'last' might be not deterministic.
 */
public class HighlanderCombineFn extends CombineFn<StationDetails, StationDetails, StationDetails> {
    private static final long serialVersionUID = 1L;

    @Override
    public StationDetails createAccumulator() {
        // Empty object is fine as accumulator as it is never used
        return new StationDetails();
    }

    @Override
    public StationDetails addInput(StationDetails accumulator, StationDetails input) {
        // Just takes the next element as the new one. No merge with the empty accumulator required
        return input;
    }

    @Override
    public StationDetails mergeAccumulators( Iterable<StationDetails> accumulators) {
        // Just use the first element that comes around
        return accumulators.iterator().next();
    }

    @Override
    public StationDetails extractOutput(StationDetails accumulator) {
        return accumulator;
    }
}
```
and is used in the pipeline via

```java
// ... and make it available as view
final PCollectionView<Map<String, StationDetails>> isdHistoryView = isdHistory.apply(Combine.perKey(new HighlanderCombineFn().<String>asKeyedFn())).apply(View.<String, StationDetails>asMap());
```

The main `PCollection` and the side input are combined into this container class

```java
package com.sungard.advtech.data;

public class WeatherAtStation extends StationDetails implements java.io.Serializable {
    private static final long serialVersionUID = 1L;
    int year;
    int month;
    int day;
    float meanTemp;
    public int getYear() {
        return year;
    }
    public void setYear(int year) {
        this.year = year;
    }
    public int getMonth() {
        return month;
    }
    public void setMonth(int month) {
        this.month = month;
    }
    public int getDay() {
        return day;
    }
    public void setDay(int day) {
        this.day = day;
    }
    public float getMeanTemp() {
        return meanTemp;
    }
    public void setMeanTemp(float meanTemp) {
        this.meanTemp = meanTemp;
    }
    public static WeatherAtStation of(int year, int month, int day, float meanTemp, StationDetails stationDetails) {
        WeatherAtStation ws = new WeatherAtStation();
        ws.year = year;
        ws.month = month;
        ws.day = day;
        ws.meanTemp = meanTemp;
        ws.lat = stationDetails.lat;
        ws.lon = stationDetails.lon;
        ws.name = stationDetails.name;
        return ws;
    }
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + day;
        result = prime * result + Float.floatToIntBits(meanTemp);
        result = prime * result + month;
        result = prime * result + year;
        return result;
    }
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        WeatherAtStation other = (WeatherAtStation) obj;
        if (day != other.day)
            return false;
        if (Float.floatToIntBits(meanTemp) != Float
                .floatToIntBits(other.meanTemp))
            return false;
        if (month != other.month)
            return false;
        if (year != other.year)
            return false;
        return true;
    }
    @Override
    public String toString() {
        return String.format("%04d-%02d-%02d: %.2f @ %s", year, month, day, meanTemp, super.toString());
    }
}
```

For basic statistics, a summary aggregator is also added. It is increased every time a station can't be found for a weather point.  
This might be because the station number is unknown, or the number might belong to a station which couldn't be parsed earlier (i.e. no geodata):

```java
package com.sungard.advtech;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.sungard.advtech.data.StationDetails;
import com.sungard.advtech.data.WeatherAtStation;

/**
 * Maps a input {@link TableRow} of the sample weather data to a {link WeatherAtStation}
 * using the side input of {@link StationDetails} generated by {@link com.sungard.advtech.ISDHistoryParser}.
 */
public class WeatherDataEnricher extends DoFn<TableRow, WeatherAtStation> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(WeatherDataEnricher.class);

    // Reference to the side input view
    private final PCollectionView<Map<String, StationDetails>> isdHistoryView;
    // Simple error tracking to see how many stations could not be resolved
    private final Aggregator<Integer, Integer> unknownStations = createAggregator("Amount of unknown stations", new Sum.SumIntegerFn());

    public WeatherDataEnricher(PCollectionView<Map<String, StationDetails>> isdHistoryView) {
        this.isdHistoryView = isdHistoryView;
    }

    @Override
    public void processElement(ProcessContext context) {
        TableRow row = context.element();
        // Extract all the interesting data
        String stationNumber = row.get("station_number").toString();
        int year = Integer.parseInt(row.get("year").toString());
        int month = Integer.parseInt(row.get("year").toString());
        int day = Integer.parseInt(row.get("day").toString());
        float meanTemp = Float.parseFloat(row.get("mean_temp").toString());

        // Lookup geo data from the side input
        Map<String, StationDetails> isdHistory = context.sideInput(isdHistoryView);
        StationDetails stationDetails = isdHistory.get(stationNumber);

        // Create the internal data structure if all data is available
        if (stationDetails != null) {
            context.output(WeatherAtStation.of(year, month, day, meanTemp, stationDetails));
        } else {
            // Otherwise just increase the counter for simple error tracking
            unknownStations.addValue(1);
            LOG.warn("Could not find station {}", stationNumber);
        }
    }
}
```

This is added to the pipeline, next to its side input:

```java
// Enrich the weather data with geo data via side input
PCollection<WeatherAtStation> weatherWithStation = weatherData.apply(
    ParDo.named("Enrich weather data with station details")
        .withSideInputs(isdHistoryView).of(new WeatherDataEnricher(isdHistoryView)));
```

**Hint:** Side inputs are immutable and are stored in the memory of each worker. They are therefore a perfect fit for simple lookup data structures.

## Filtering and additional Aggregating Logic
Filtering is a powerful tool within Dataflow. The SDK comes with a various predefined comparison functions for numbers.

Since we want to filter for weather around New York, a custom filter is needed:

```java
package com.sungard.advtech;

import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.sungard.advtech.data.WeatherAtStation;

/**
 * Filter to find {@link WeatherAtStation} which are within 1 degree range of
 * +40.753876/-73.978777 location wise.
 */
public class NewYorkFilterFn implements SerializableFunction<WeatherAtStation, Boolean> {
    private static final long serialVersionUID = 1L;

    // Filter for anything that is within 1 degree = 69 miles/111 km of home
    final float TARGET_LAT = +40.753876f;
    final float TARGET_LON = -73.978777f;
    final float TARGET_JITTER = 1.0f;

    private boolean isClose(float target, float actual) {
        return actual < target + TARGET_JITTER &&
                actual > target - TARGET_JITTER;
    }

    @Override
    public Boolean apply(WeatherAtStation input) {
        return isClose(TARGET_LAT, input.getLat())
            && isClose(TARGET_LON, input.getLon());
    }
}
```

and added to the pipeline:

```java
// Filter the weather down to a specific location...
PCollection<WeatherAtStation> weatherInNewNewYork = weatherWithStation.apply(
    Filter.by(new NewYorkFilterFn()));
```

## Output Logic 1 - Only Aggregators
For the New York weather, the minimum and maximum temperature is simply aggregated using Min and Max aggregators:

```java
package com.sungard.advtech;

import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Max;
import com.google.cloud.dataflow.sdk.transforms.Min;
import com.sungard.advtech.data.WeatherAtStation;

/**
 * Composition of a min and a max aggregator just for collecting purposes.
 * This DoFn is functional-wise a sink and does not emit any data.
 */
public class MinMaxAggregators extends DoFn<WeatherAtStation, Void> {
    private static final long serialVersionUID = 1L;
    private final Aggregator<Double, Double> minTemp = createAggregator("Min Temperatur in New York", new Min.MinDoubleFn());
    private final Aggregator<Double, Double> maxTemp = createAggregator("Max Temperatur in New York", new Max.MaxDoubleFn());

    @Override
    public void processElement(ProcessContext context) {
        WeatherAtStation ws = context.element();

        // Let the aggregator do the heavy lifting
        minTemp.addValue(new Double(ws.getMeanTemp()));
        maxTemp.addValue(new Double(ws.getMeanTemp()));

        //Nothing gets emitted here. Maybe write also to GCS if you want
    }
}
```

Writing to (for example) GCS is left as an exercise for the reader. For the moment it is just added to the pipeline as is:

```java
// ... and aggreagte min/max temperatur
weatherInNewNewYork.apply(ParDo.named("Find min-max temperatur in New York")
    .of(new MinMaxAggregators()));
```

## Output Logic 2 - Side outputs
Side outputs are the opposite of side inputs and allow a step within a pipeline to have multiple outputs:

```java
package com.sungard.advtech;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.sungard.advtech.data.WeatherAtStation;

/**
 * Maps the {@link WeatherAtStation} internal representation of weather data
 * to the output-compatible formats for Bigtable ({@link Put}) and BigQuery ({@link TableRow}).
 */
public class OutputCreator extends DoFn<WeatherAtStation, Mutation> {
    private static final long serialVersionUID = 1L;
    /** TupleTag reference to write the side output */
    private final TupleTag<TableRow> bigQueryOutput;

    public OutputCreator(TupleTag<TableRow> bigQueryOutput) {
        this.bigQueryOutput = bigQueryOutput;
    }

    /** Generates a rowkey for BigTable allowing geo-wise aggregation of rows */
    String generateBigtableKey(WeatherAtStation ws) {
        return String.format("%+09.4f|%+09.4f|%04d-%02d-%02d", ws.getLat(), ws.getLon(), ws.getYear(), ws.getMonth(), ws.getDay());
    }

    void outputToBigtable(ProcessContext context) {
        WeatherAtStation ws = context.element();
        // Create a unique key that groups the weather based on the location
        byte[] rowKey = generateBigtableKey(ws).getBytes();

        // Always set the timestamp. This allows the Bigtable API to do retries internally during RuntimeExceptions
        Put put = new Put(rowKey, System.currentTimeMillis());

        // Write some additional data to the column families
        put.addColumn("location".getBytes(), "Station Name".getBytes(), ws.getName().getBytes());
        put.addColumn("weather".getBytes(), "mean_temp".getBytes(), Float.toString(ws.getMeanTemp()).getBytes());

        context.output(put);
    }

    void outputToBigQuery(ProcessContext context) {
        WeatherAtStation ws = context.element();

        // Create a row that should be saved. No further processing
        TableRow outputRow = new TableRow();
        outputRow.set("stationName", ws.getName());
        outputRow.set("year", ws.getYear());
        outputRow.set("month", ws.getMonth());
        outputRow.set("day", ws.getDay());
        outputRow.set("meanTemp", ws.getMeanTemp());

        context.sideOutput(bigQueryOutput, outputRow);
    }

    @Override
    public void processElement(ProcessContext context) {
        outputToBigtable(context);
        outputToBigQuery(context);
    }

    /** Provides the schema structure for the BigQuery {@link TableRow} this Function is generating */
    public static TableSchema getBigQuerySchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("stationName").setType("STRING"));
        fields.add(new TableFieldSchema().setName("year").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("month").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("day").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("meanTemp").setType("FLOAT"));

        TableSchema schema = new TableSchema();
        schema.setFields(fields);

        return schema;
    }
}
```

and is added as the final processing piece to the pipeline:

**Hint:** As all the data should be stored, `weatherAtStation` is used again - it was also used for the New York filtering but within Dataflow it is absolutely fine to reuse `PCollection`s

```java
// Create tuples as anonymous classes for side outputs
final TupleTag<TableRow> bigQueryOutput = new TupleTag<TableRow>(){private static final long serialVersionUID = 1L;};
final TupleTag<Mutation> bigTableOutput = new TupleTag<Mutation>(){private static final long serialVersionUID = 1L;};
// Create the two (main+side) outputs for Bigtable and BigQuery output
PCollectionTuple combinedOutput = weatherWithStation.apply(ParDo.named("Prepare write")
        .withOutputTags(bigTableOutput, TupleTagList.of(bigQueryOutput))
        .of(new OutputCreator(bigQueryOutput)));

/* Help DataFlow on how to serialize the elements of the output.
   Can't be interfered due to Java's type erasure */
combinedOutput.get(bigQueryOutput).setCoder(TableRowJsonCoder.of());
combinedOutput.get(bigTableOutput).setCoder(new HBaseMutationCoder());
```

**Hint:** Due to Java's type eraser, for side outputs two things must be considered:

1. The `TagTuple` must be used as an anonymous class.
1. Depending on the type in the output, it might be required to register appropriate Coders on the output. Dataflow might not be able to interfere with the type during the pipeline validation. For datatypes from the Google ecosystem, matching coders already exist in `com.google.cloud.dataflow.sdk.coders` and `com.google.cloud.bigtable.dataflow`.

## Write Logic 1 - Bigtable
Writing to Bigtable should be done via the provided classes of the SDK.

It is also possible to use the HBase API within Dataflow, but this requires that your code can handle the connection management. Due to the asynchronous nature of the gRPC protocol used below, this should be avoided if it is not required:

The table that should be written to will be provided via the command line arguments. The table itself was created during the [basic setup](#basic-setup)

```java
// Write the main output to Bigtable
combinedOutput.get(bigTableOutput).apply(
    CloudBigtableIO.writeToTable(CloudBigtableTableConfiguration.fromCBTOptions(options)));
```

## Write Logic 2 - BigQuery
Writing to BigQuery is also supported via SDK-bundled classes.
The schema can be created as needed. For this example the table is always re-created when the pipeline is executed.

```java
// Write the side output to BigQuery
combinedOutput.get(bigQueryOutput).apply(BigQueryIO.Write.named("Write to BigQuery")
    .to(options.getBigQueryTable())
    .withSchema(OutputCreator.getBigQuerySchema()) // Provide the schema so the table can be autocreated
    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
```

**Hint:** Keep in mind that BigQuery tables are immutable!

## Testing the Pipes
For testing, Dataflow provides [several classes to support unit testing](https://cloud.google.com/dataflow/pipelines/testing-your-pipeline) of the various pieces of a pipeline.
As an example, the `OutputCreator` is tested - as it accounts for most of the Dataflow API usage - using the classic JUnit/Hamcrest testing style:

```xml
<dependency>
	<groupId>junit</groupId>
	<artifactId>junit</artifactId>
	<version>4.12</version>
	<scope>test</scope>
</dependency>
<dependency>
	<groupId>org.hamcrest</groupId>
	<artifactId>hamcrest-all</artifactId>
	<version>1.3</version>
	<scope>test</scope>
</dependency>
```

Combining the test classes provided by the SDK with the JUnit annotations

```java
package com.sungard.advtech;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.apache.hadoop.hbase.client.Mutation;
import org.junit.Before;
import org.junit.Test;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TupleTagList;
import com.sungard.advtech.data.StationDetails;
import com.sungard.advtech.data.WeatherAtStation;

/**
 * Test via a class JUnit test directly and a wrapper version via {@link DoFnTester}
 */
public class OutputCreatorTest {
    /** Use as static class, not as anonymous class! */
    private static TupleTag<TableRow> bigQueryOutput = new TupleTag<TableRow>(){private static final long serialVersionUID = 1L;};

    private OutputCreator underTestUnderlying;

    @Before
    public void setUp() {
        underTestUnderlying = new OutputCreator(bigQueryOutput);
    }

    /** Test simple method calls on the object right away - no DataFlow API is involved */
    @Test
    public void testKeyGeneration() {
        WeatherAtStation ws = WeatherAtStation.of(2015, 9, 28, 23.4f,
            StationDetails.of("testStation", 23.456789f, -42.101234f));
        String actual = underTestUnderlying.generateBigtableKey(ws);

        assertEquals("+023.4568|-042.1012|2015-09-28", actual);
    }

    /** Test via DataFlow API processing of a single input element */
    @Test
    public void testBigQuerySideOutput() {
        DoFnTester<WeatherAtStation, Mutation> underTest = DoFnTester.of(underTestUnderlying);
        underTest.setSideOutputTags(TupleTagList.of(bigQueryOutput));

        underTest.processBatch(WeatherAtStation.of(2015, 9, 28, 23.456f,
                StationDetails.of("testStation", 23.456789f, -42.101234f)));

        List<TableRow> actual = underTest.takeSideOutputElements(bigQueryOutput);

        TableRow tr = new TableRow();
        tr.set("stationName", "testStation");
        tr.set("year", 2015);
        tr.set("month", 9);
        tr.set("day", 28);
        tr.set("meanTemp", 23.456f);
        assertThat(actual, hasItems(new TableRow[]{tr}));
    }
}
```

or using a full-fledged `TestPipeline` that is executed locally to check how different `DoFn`s interact:

```java
package com.sungard.advtech;

import org.hamcrest.CoreMatchers;
import org.hamcrest.collection.IsIterableWithSize;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.sungard.advtech.data.StationDetails;

/**
 * Test to show {@link TestPipeline} and Hamcrest usage
 */
public class ISDHistoryParserTest {
    /** Use as static class, not as anonymous class! */
    static class AssertContentIsCorrect implements SerializableFunction<Iterable<KV<String, StationDetails>>, Void> {
        private static final long serialVersionUID = 1L;

        @Override
        public Void apply(Iterable<KV<String, StationDetails>> input) {
            // Check for various asserts in one go
            Assert.assertThat(input, CoreMatchers.allOf(
                // Check for the correct length
                IsIterableWithSize.<KV<String, StationDetails>>iterableWithSize(28003),
                IsCollectionContaining.<KV<String, StationDetails>>hasItems(
                    // Check for two items in the list
                    KV.of("010010", StationDetails.of("JAN MAYEN(NOR-NAVY)", +70.933f, -008.667f)),
                    KV.of("999710", StationDetails.of("LIBAVA",  +049.670f, +017.480f)))
                ));
            return null;
        }
    }

    private ISDHistoryParser underTest;

    @Before
    public void setUp() {
        underTest = new ISDHistoryParser();
    }

    @Test
    public void parseISDHistoryFile() {
        // Create a test pipeline that runs locally
        Pipeline p = TestPipeline.create();

        // Build a local PCollection based on a local file
        PCollection<KV<String, StationDetails>> actual = p
            .apply(TextIO.Read.from("src/test/resources/isd-history.csv"))
            // and process via the test subject
            .apply(ParDo.of(underTest));

        // Assert on the results
        DataflowAssert.that(actual).satisfies(new AssertContentIsCorrect());

        // Run the pipeline.
        p.run();
    }
}
```

**Hint:** It should be ensured that no anonymous classes but static inner classes are used. Otherwise execution of the `TestPipeline` will cause serialization exceptions.

The test cases can be run via the classic
```
mvn test
```

## Running it
To execute the pipeline, the java program has to be executed.
When Dataflow starts, some parts are validated. This includes checking for the existence of the Bigtable table. The communication requires ALPN on the *boot* classpath. Details can be found [here](http://www.eclipse.org/jetty/documentation/current/alpn-chapter.html).
To achieve this, a non-invasive approach is to add profiles to the `pom.xml` in order to load the correct ALPN version:

```xml
<dependencies>
...
	<dependency>
		<groupId>org.mortbay.jetty.alpn</groupId>
		<artifactId>alpn-boot</artifactId>
		<!-- needs to be on the boot classpath, therefore 'provided' scope -->
		<scope>provided</scope>
		<version>${alpn.version}</version>
	</dependency>
</dependencies>
<profiles>
<!-- http://www.eclipse.org/jetty/documentation/current/alpn-chapter.html#alpn-versions -->
	<profile>
		<id>1.7.0u40-1.7.0u67</id>
		<activation>
			<jdk>[1.7.0.40,1.7.0.67]</jdk>
		</activation>
		<properties>
			<alpn.version>7.1.0.v20141016</alpn.version>
		</properties>
	</profile>
	<profile>
		<id>1.7.0u71-1.7.0u72</id>
		<activation>
			<jdk>[1.7.0.71,1.7.0.72]</jdk>
		</activation>
		<properties>
			<alpn.version>7.1.2.v20141202</alpn.version>
		</properties>
	</profile>
	<profile>
		<id>1.7.0u75-1.7.0u80</id>
		<activation>
			<jdk>[1.7.0.75,1.7.0.80]</jdk>
		</activation>
		<properties>
			<alpn.version>7.1.3.v20150130</alpn.version>
		</properties>
	</profile>
	<profile>
		<id>1.8.0-1.8.0u20</id>
		<activation>
			<jdk>[1.8.0.0,1.8.0.20]</jdk>
		</activation>
		<properties>
			<alpn.version>8.1.0.v20141016</alpn.version>
		</properties>
	</profile>
	<profile>
		<id>1.8.0u25</id>
		<activation>
			<jdk>1.8.0.25</jdk>
		</activation>
		<properties>
			<alpn.version>8.1.2.v20141202</alpn.version>
		</properties>
	</profile>
	<profile>
		<id>1.8.0u31-1.8.0u45</id>
		<activation>
			<jdk>[1.8.0.31,1.8.0.45]</jdk>
		</activation>
		<properties>
			<alpn.version>8.1.3.v20150130</alpn.version>
		</properties>
	</profile>
	<profile>
		<id>1.8.0u51</id>
		<activation>
			<jdk>1.8.0.51</jdk>
		</activation>
		<properties>
			<alpn.version>8.1.4.v20150727</alpn.version>
		</properties>
	</profile>
</profiles>
```

To allow for the easier replication of execution runs, the Maven plugin `exec` is added to the `pom.xml`.

Dataflow allows a lot of customization, the following settings are used for this run:

```xml
<properties>
	<!-- The GCP project id -->
	<gc.projectId>YOUR_PROJECT_ID</gc.projectId>
	<!-- A GCS bucket that will be used by Dataflow to upload the pipeline code -->
	<gc.stagingLocation>gs://noaa</gc.stagingLocation>
	<!-- Name of the Bigtable cluster created earlier
	<gc.btClusterId>weather</gc.btClusterId>
	<!-- Location of the Bigtable cluster -->
	<gc.btZoneId>us-central1-b</gc.btZoneId>

	<!-- Where to read the ISD history file from -->
	<gc.isdHistory>gs://noaa/isd-history.csv</gc.isdHistory>
	<!-- Bigtable table name to write the output to -->
	<gc.btTable>weather</gc.btTable>
	<!-- BigQuery dataset/table name to write the output to -->
	<gc.bqTable>YOUR_PROJECT_ID:weather.outputtable</gc.bqTable>

	<!-- Dataflow provided different autoscaling methods based on the throughput -->
	<gc.dfAutoscalingAlgorithm>THROUGHPUT_BASED</gc.dfAutoscalingAlgorithm>
	<!-- Starting the pipeline with 3 workers. Might be scaled up or down depending on the pipeline throughput -->
	<gc.numWorkers>3</gc.numWorkers>
	<!-- Type of the pipeline worker. Default is n1 -->
	<gc.workerMachineType>n1-standard-4</gc.workerMachineType>
	<!-- Disk size of the pipeline worker. 10GiB is the minimum -->
	<gc.workerDiskSize>10</gc.workerDiskSize>
	<!-- Zone where the worker nodes are created. Same as the Bigtable zone for improved performance -->
	<gc.zone>us-central1-b</gc.zone>
</properties>
```

**Hint:** The Dataflow require the regular GCP resources for IPs, Disk and CPU. If the quota is reached, the autoscaling does not take place, but no warning is logged.

```xml
<plugin>
	<groupId>org.codehaus.mojo</groupId>
	<artifactId>exec-maven-plugin</artifactId>
	<configuration>
		<executable>java</executable>
		<arguments>
			<argument>-Xbootclasspath/p:${settings.localRepository}/org/mortbay/jetty/alpn/alpn-boot/${alpn.version}/alpn-boot-${alpn.version}.jar</argument>
			<argument>-classpath</argument>
			<classpath />
			<argument>com.sungard.advtech.CloudyWithMeatballs</argument>
			<argument>--runner=BlockingDataflowPipelineRunner</argument>
			<argument>--project=${gc.projectId}</argument>
			<argument>--stagingLocation=${gc.stagingLocation}</argument>

			<argument>--bigtableProjectId=${gc.projectId}</argument>
			<argument>--bigtableClusterId=${gc.btClusterId}</argument>
			<argument>--bigtableZoneId=${gc.btZoneId}</argument>

			<argument>--isdHistory=${gc.isdHistory}</argument>
			<argument>--bigtableTableId=${gc.btTable}</argument>
			<argument>--bigQueryTable=${gc.bqTable}</argument>

			<argument>--autoscalingAlgorithm=${gc.dfAutoscalingAlgorithm}</argument>
			<argument>--numWorkers=${gc.numWorkers}</argument>
			<!-- There is a bug in DataFlow. We have to set 500 to imply "unbound" -->
			<argument>--maxNumWorkers=500</argument>
			<argument>--workerMachineType=${gc.workerMachineType}</argument>
			<argument>--diskSizeGb=${gc.workerDiskSize}</argument>
		</arguments>
	</configuration>
</plugin>
```

and run this via
```
mvn clean compile exec:exec
```