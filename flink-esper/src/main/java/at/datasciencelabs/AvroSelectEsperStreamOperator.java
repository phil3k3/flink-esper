package at.datasciencelabs;

import com.espertech.esper.client.ConfigurationEventTypeAvro;
import com.espertech.esper.client.ConfigurationOperations;
import com.espertech.esper.client.EPRuntime;
import org.apache.avro.generic.GenericData;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * An operator which supports detecting event sequences and patterns using Esper.
 * The operator expects the events being Avro records.
 *
 * @param <KEY> Type of the key
 * @param <IN>  Type of the input stream
 * @param <OUT> Type of the output stream
 */
public class AvroSelectEsperStreamOperator<KEY, IN extends GenericData.Record, OUT> extends SelectEsperStreamOperator<KEY, IN, OUT> {

	private static final long serialVersionUID = -1443208504064873118L;

	/** Optional type mapping to override the default POJO mapping */
	private final SchemaProvider typeMapping;

	/**
	 * Constructs a new operator. Requires the type of the input DataStream to register its Event Type at Esper.
	 * Currently only processing time evaluation is supported.
	 *
	 * @param inputStreamType     type of the input DataStream
	 * @param esperSelectFunction function to select from Esper's output
	 * @param isProcessingTime    Flag indicating how time is interpreted (processing time vs event time)
	 * @param esperQuery          The esper query
	 * @param schemaProvider    Type mapping provider to enable non-Pojo event types
	 */
	public AvroSelectEsperStreamOperator(TypeInformation<IN> inputStreamType, EsperSelectFunction<OUT> esperSelectFunction, boolean isProcessingTime, EsperStatementFactory esperQuery, SchemaProvider schemaProvider) {
		super(inputStreamType, esperSelectFunction, isProcessingTime, esperQuery);
		this.typeMapping = schemaProvider;
	}

	/**
	 * Configures the Esper context by adding the available Avro event types.
	 * @param configurationOperations The configuration which will be extended.
	 */
	@Override
	protected void configure(ConfigurationOperations configurationOperations) {
		typeMapping.getEventTypes().forEach((type, schema) -> {
			ConfigurationEventTypeAvro avroEvent = new ConfigurationEventTypeAvro(schema);
			configurationOperations.addEventTypeAvro(type, avroEvent);
		});
	}

	/**
	 * Send an event to the Esper runtime.
	 *
	 * @param streamRecord The record containing the event which will be sent
	 * @param runtime      The target runtime
	 */
	@Override
	protected void sendEvent(StreamRecord<IN> streamRecord, EPRuntime runtime) {
		runtime.sendEventAvro(streamRecord.getValue(), streamRecord.getValue().getSchema().getName());
	}
}
