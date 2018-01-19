package at.datasciencelabs;

import org.apache.avro.generic.GenericData;
import org.apache.flink.streaming.api.datastream.DataStream;

import at.datasciencelabs.mapping.EsperTypeMapping;

public class TypedEsperStream<IN extends GenericData.Record> extends EsperStream<IN> {

	private final EsperTypeMapping esperTypeMapping;

	/**
	 * Create a new EsperStream instance.
	 *
	 * @param inputStream The input DataStream
	 * @param esperQuery  An Esper query
	 */
	TypedEsperStream(DataStream<IN> inputStream, EsperStatementFactory esperQuery, EsperTypeMapping esperTypeMapping) {
		super(inputStream, esperQuery);
		this.esperTypeMapping = esperTypeMapping;
	}

	@Override
	protected <R> SelectEsperStreamOperator<Byte, IN, R> getOperator(EsperSelectFunction<R> esperSelectFunction, boolean isProcessingTime) {
		return new TypedSelectEsperStreamOperator<>(inputStream.getType(), esperSelectFunction, isProcessingTime, esperQuery, esperTypeMapping);
	}
}
