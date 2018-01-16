package at.datasciencelabs;

import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Utility class for complex event processing using Esper.
 *
 * <p>Methods which transform a {@link DataStream} into a {@link EsperStream} to do CEP.
 */
public class Esper {

    /**
     * Creates a {@link EsperStream} from an input data stream and a pattern.
     *
     * @param input DataStream containing the input events
     * @param pattern Esper pattern specification which shall be detected
     * @param <IN> Type of the input events
     * @return Resulting esper stream
     */
    public static <IN> EsperStream<IN> pattern(DataStream<IN> input, String pattern) {
        return new EsperStream<IN>(input, new EsperPattern(pattern));
    }

    /**
     * Creates a {@link EsperStream} from an input data stream and a query.
     *
     * @param input DataStream containing the input events
     * @param query Query of describing which events should be selected from the stream
     * @param <IN> Type of the input events
     * @return Resulting esper stream
     */
    public static <IN> EsperStream<IN> query(DataStream<IN> input, String query) {
        return new EsperStream<IN>(input, new EsperQuery(query));
    }
}
