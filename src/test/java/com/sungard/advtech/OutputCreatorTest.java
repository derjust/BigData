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
