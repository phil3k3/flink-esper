package at.datasciencelabs;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;


public class EsperStream<IN> {

    private final DataStream<IN> input;
    private final String query;


    public EsperStream(DataStream<IN> input, String query) {
        this.input = input;
        this.query = query;
    }

    /**
     * Select from the EsperStream
     */
    public <R> SingleOutputStreamOperator<R> select(EsperSelectFunction<R> selectFunction, TypeInformation<R> returnType) {
        KeySelector<IN, Byte> keySelector = new NullByteKeySelector<>();

        SingleOutputStreamOperator<R> patternStream;

        // TODO until the typeextractor is capable of extracing non-generic parameters, the return type has to be passed in manually

        final boolean isProcessingTime = input.getExecutionEnvironment().getStreamTimeCharacteristic() == TimeCharacteristic.ProcessingTime;
        patternStream = input.keyBy(keySelector).transform("SelectEsperOperator", returnType, new SelectEsperStreamOperator<Byte, IN, R>(input.getType(), selectFunction, isProcessingTime, query));

        return patternStream;
    }


}
