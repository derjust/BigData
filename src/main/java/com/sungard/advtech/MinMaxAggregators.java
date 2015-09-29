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