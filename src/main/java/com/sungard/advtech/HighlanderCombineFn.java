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