package at.datasciencelabs;

import org.apache.flink.streaming.api.datastream.DataStream;

public class Esper {

    public static <IN> EsperStream<IN> pattern(DataStream<IN> dataStream, String query) {
        return new EsperStream<IN>(dataStream, query);
    }
}
