package at.datasciencelabs;

import org.apache.avro.generic.GenericData;
import org.apache.flink.streaming.api.datastream.DataStream;

import at.datasciencelabs.mapping.EsperTypeMapping;

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
     * @param <IN2> Type of the input events
     * @return Resulting esper stream
     */
    public static <IN2 extends GenericData.Record> EsperStream<IN2> pattern(DataStream<IN2> input, String pattern, EsperTypeMapping mapping) {
        return new TypedEsperStream<>(input, new EsperPattern(pattern), mapping);
    }

    /**
     * Creates a {@link EsperStream} from an input data stream and a pattern.
     *
     * @param input DataStream containing the input events
     * @param pattern Esper pattern specification which shall be detected
     * @param <IN> Type of the input events
     * @return Resulting esper stream
     */
    public static <IN> EsperStream<IN> pattern(DataStream<IN> input, String pattern) {
        return new EsperStream<>(input, new EsperPattern(pattern));
    }


    /**
     * Creates a {@link EsperStream} from an input data stream and a query.
     *
     * @param input DataStream containing the input events
     * @param query Query of describing which events should be selected from the stream
     * @param <IN2> Type of the input events
     * @return Resulting esper stream
     */
    public static <IN2 extends GenericData.Record> EsperStream<IN2> query(DataStream<IN2> input, String query, EsperTypeMapping mapping) {
        return new TypedEsperStream<>(input, new EsperQuery(query), mapping);
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
        return new EsperStream<>(input, new EsperQuery(query));
    }
}
