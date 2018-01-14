package at.datasciencelabs;

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractionException;
import org.apache.flink.api.java.typeutils.TypeExtractionUtils;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.lang.reflect.Type;


/**
 * A DataStream which is able to detect event sequences and patterns using Esper
 * @see <a href="www.espertech.com">www.espertech.com</a> for detailed documentation.
 */
public class EsperStream<IN> {

    private final DataStream<IN> inputStream;
    private final EsperStatementFactory esperQuery;


    /**
     * Create a new EsperStream instance.
     * @param inputStream The input DataStream
     * @param esperQuery An Esper query
     */
    EsperStream(DataStream<IN> inputStream, EsperStatementFactory esperQuery) {
        this.inputStream = inputStream;
        this.esperQuery = esperQuery;
    }

    /**
     * Applies a select function to the detected pattern sequence or query results. For each pattern sequence or query result the
     * provided {@link EsperSelectFunction} is called. The pattern select function can produce
     * exactly one resulting element.
     *
     * @param esperSelectFunction The pattern select function which is called for each detected pattern sequence.
     * @param <R> Type of the resulting elements
     * @return {@link DataStream} which contains the resulting elements from the pattern select
     *         function.
     */
    public <R> SingleOutputStreamOperator<R> select(EsperSelectFunction<R> esperSelectFunction) {
        KeySelector<IN, Byte> keySelector = new NullByteKeySelector<>();

        SingleOutputStreamOperator<R> patternStream;

        TypeInformation<R> typeInformation = getTypeInformation(esperSelectFunction);

        final boolean isProcessingTime = inputStream.getExecutionEnvironment().getStreamTimeCharacteristic() == TimeCharacteristic.ProcessingTime;
        patternStream = inputStream.keyBy(keySelector).transform("SelectEsperOperator", typeInformation, new SelectEsperStreamOperator<Byte, IN, R>(inputStream.getType(), esperSelectFunction, isProcessingTime, esperQuery));

        return patternStream;
    }

    @SuppressWarnings("unchecked")
    private <OUT> TypeInformation<OUT> getTypeInformation(EsperSelectFunction<OUT> esperSelectFunction) {
        try {
            TypeExtractionUtils.LambdaExecutable lambdaExecutable = TypeExtractionUtils.checkAndExtractLambda(esperSelectFunction);
            if (esperSelectFunction instanceof ResultTypeQueryable) {
                return ((ResultTypeQueryable<OUT>) esperSelectFunction).getProducedType();
            }
            if (lambdaExecutable != null) {
                Type type = lambdaExecutable.getReturnType();
                return (TypeInformation<OUT>) TypeExtractor.createTypeInfo(type);
            }
            else {
                return TypeExtractor.createTypeInfo(esperSelectFunction, EsperSelectFunction.class, esperSelectFunction.getClass(), 0);
            }
        } catch (TypeExtractionException e) {
            throw new InvalidTypesException("Could not extract types.", e);
        }
    }

}
