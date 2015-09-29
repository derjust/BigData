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