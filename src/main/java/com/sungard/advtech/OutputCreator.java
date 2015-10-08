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