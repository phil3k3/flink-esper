package at.datasciencelabs;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;


/**
 * A DataStream which is able to detect event sequences and patterns using Esper
 * @see <a href="www.espertech.com">www.espertech.com</a> for detailed documentation.
 */
public class EsperStream<IN> {

    private final DataStream<IN> inputStream;
    private final String esperQuery;


    /**
     * Create a new EsperStream instance.
     * @param inputStream The input DataStream
     * @param esperQuery An Esper query
     */
    public EsperStream(DataStream<IN> inputStream, String esperQuery) {
        this.inputStream = inputStream;
        this.esperQuery = esperQuery;
    }

    /**
     * Select from the EsperStream, must provide the return type of the output DataStream since no type information is
     * currently extracted from the @see {@link EsperSelectFunction}.
     */
    public <R> SingleOutputStreamOperator<R> select(EsperSelectFunction<R> esperSelectFunction, TypeInformation<R> dataStreamReturnType) {
        KeySelector<IN, Byte> keySelector = new NullByteKeySelector<>();

        SingleOutputStreamOperator<R> patternStream;

        // TODO until the typeextractor is capable of extracing non-generic parameters, the return type has to be passed in manually

        final boolean isProcessingTime = inputStream.getExecutionEnvironment().getStreamTimeCharacteristic() == TimeCharacteristic.ProcessingTime;
        patternStream = inputStream.keyBy(keySelector).transform("SelectEsperOperator", dataStreamReturnType, new SelectEsperStreamOperator<Byte, IN, R>(inputStream.getType(), esperSelectFunction, isProcessingTime, esperQuery));

        return patternStream;
    }

}
