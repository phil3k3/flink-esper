package at.datasciencelabs;

import org.apache.avro.generic.GenericData;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * A DataStream which is able to detect event sequences and patterns using Esper
 * @see <a href="www.espertech.com">www.espertech.com</a> for detailed documentation.
 *
 * The stream expects the events being Avro records.
 */
public class AvroEsperStream<IN extends GenericData.Record> extends EsperStream<IN> {

	private final SchemaProvider schemaProvider;

	/**
	 * Create a new EsperStream instance.
	 *
	 * @param inputStream The input DataStream
	 * @param esperQuery  An Esper query
	 */
	AvroEsperStream(DataStream<IN> inputStream, EsperStatementFactory esperQuery, SchemaProvider schemaProvider) {
		super(inputStream, esperQuery);
		this.schemaProvider = schemaProvider;
	}

	@Override
	protected <R> SelectEsperStreamOperator<Byte, IN, R> getOperator(EsperSelectFunction<R> esperSelectFunction, boolean isProcessingTime) {
		return new AvroSelectEsperStreamOperator<>(inputStream.getType(), esperSelectFunction, isProcessingTime, esperQuery, schemaProvider);
	}
}
