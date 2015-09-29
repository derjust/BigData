package com.sungard.advtech.data;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class WeatherAtStationTest {

    @Test
    public void testToString() {
        WeatherAtStation underTest = WeatherAtStation.of(2015, 9, 28, 23.456f,
                StationDetails.of("testStation", 23.456789f, -42.101234f));

        String actual = underTest.toString();

        assertEquals("2015-09-28: 23.46 @ testStation: +023.4568|-042.1012", actual);
    }
}
