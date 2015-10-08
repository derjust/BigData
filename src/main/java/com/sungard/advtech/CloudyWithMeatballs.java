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

        // Prepare the side input...
        PCollection<KV<String, StationDetails>> isdHistory = p
            .apply(TextIO.Read.named("Read ISD History").from(options.getIsdHistory()))
            .apply(ParDo.named("Parse ISD History").of(new ISDHistoryParser()));
        // ... and make it available as view
        final PCollectionView<Map<String, StationDetails>> isdHistoryView = isdHistory.apply(Combine.perKey(new HighlanderCombineFn().<String>asKeyedFn())).apply(View.<String, StationDetails>asMap());

        // Read the main PColleciton from BigQuery
        PCollection<TableRow> weatherData = p.apply(
            BigQueryIO.Read.named("Read Weather Data")
              .fromQuery("SELECT wban_number, station_number, year, month, day, mean_temp, min_temperature, max_temperature"
              + " FROM publicdata:samples.gsod"));

        // Enrich the weather data with geo data via side input
        PCollection<WeatherAtStation> weatherWithStation = weatherData.apply(
            ParDo.named("Enrich weather data with station details")
                .withSideInputs(isdHistoryView).of(new WeatherDataEnricher(isdHistoryView)));

        // Filter the weather down to a specific location...
        PCollection<WeatherAtStation> weatherInNewNewYork = weatherWithStation.apply(
            Filter.by(new NewYorkFilterFn()));
        // ... and aggreagte min/max temperatur
        weatherInNewNewYork.apply(ParDo.named("Find min-max temperatur in New York")
            .of(new MinMaxAggregators()));

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

        // Write the main output to Bigtable
        combinedOutput.get(bigTableOutput).apply(
                CloudBigtableIO.writeToTable(CloudBigtableTableConfiguration.fromCBTOptions(options)));

        // Write the side output to BigQuery
        combinedOutput.get(bigQueryOutput).apply(BigQueryIO.Write.named("Write to BigQuery")
            .to(options.getBigQueryTable())
            .withSchema(OutputCreator.getBigQuerySchema()) // Provide the schema so the table can be autocreated
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        p.run();
    }

}
