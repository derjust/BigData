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