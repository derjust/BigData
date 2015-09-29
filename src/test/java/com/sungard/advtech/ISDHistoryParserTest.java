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
