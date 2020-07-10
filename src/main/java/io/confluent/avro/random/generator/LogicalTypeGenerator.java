package io.confluent.avro.random.generator;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class LogicalTypeGenerator {

    static final DateFormat ISO_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    /**
     * The name of the attribute for specifying a possible range of values of iso-date strings. Must be
     * given as an object.
     */
    public static final String DATE_RANGE_PROP = "range";
    /**
     * The name of the attribute for specifying the (inclusive) minimum value in a range. Must be
     * given as a iso-date string.
     */
    public static final String DATE_RANGE_PROP_START = "start";
    /**
     * The name of the attribute for specifying the (exclusive) maximum value in a range. Must be
     * given as a iso-date string.
     */
    public static final String DATE_RANGE_PROP_END = "end";

    static {
        ISO_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private static Date dateBetween(Date startInclusive, Date endExclusive) {
        long startMillis = startInclusive.getTime();
        long endMillis = endExclusive.getTime();
        long randomMillisSinceEpoch = ThreadLocalRandom
                .current()
                .nextLong(startMillis, endMillis);

        return new Date(randomMillisSinceEpoch);
    }

    private static Date getRandomDate(String logicalType, Optional<String> expected, Optional<Date> since) {
        switch (logicalType) {
            case "iso-date":
                return expected.map(ex -> {
                    try {
                        return ISO_DATE_FORMAT.parse(ex);
                    } catch (ParseException e) {
                        throw new IllegalArgumentException("Unsupported iso-date range format: " + ex);
                    }
                }).orElseGet(() -> since.map(start -> {
                    long aDay = TimeUnit.DAYS.toMillis(1);
                    Date end = new Date(start.getTime() + aDay * 365 * 10);
                    return Date.from(Instant.ofEpochSecond(ThreadLocalRandom.current().nextLong(start.getTime(), end.getTime())));
                }).orElseGet(() -> Date.from(Instant.ofEpochSecond(ThreadLocalRandom.current().nextInt()))));
            default:
                throw new IllegalArgumentException("Illegal date based logical type: " + logicalType);
        }
    }


    public static Object random(String logicalType, Map propertiesProp) {
        switch (logicalType) {
            case "iso-date":
                Object dateRangeProp = propertiesProp.get(DATE_RANGE_PROP);
                if (dateRangeProp != null) {
                    if (dateRangeProp instanceof Map) {
                        Map dateRangeProps = (Map) dateRangeProp;
                        Date start = getRandomDate(logicalType,
                                Optional.ofNullable(dateRangeProps.get(DATE_RANGE_PROP_START)).map(Object::toString),
                                        Optional.empty());
                        Date end = getRandomDate(logicalType,
                                Optional.ofNullable(dateRangeProps.get(DATE_RANGE_PROP_END)).map(Object::toString),
                                Optional.empty());

                        if (start.after(end)) {
                            throw new RuntimeException(String.format(
                                    "'%s' field must be strictly less than '%s' field in %s property",
                                    DATE_RANGE_PROP_START,
                                    DATE_RANGE_PROP_END,
                                    DATE_RANGE_PROP
                            ));
                        }
                        return Optional.of(dateBetween(start, end))
                                .map(ISO_DATE_FORMAT::format)
                                .get();
                    }
                }
                return Optional.of(getRandomDate(logicalType, Optional.empty(), Optional.empty()))
                        .map(ISO_DATE_FORMAT::format)
                        .get();
            default:
                throw new IllegalArgumentException("Unsupported logical type: " + logicalType);
        }
    }
}
