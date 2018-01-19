package at.datasciencelabs;

import org.apache.avro.generic.GenericData;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import com.espertech.esper.client.ConfigurationEventTypeAvro;
import com.espertech.esper.client.EPServiceProvider;

import at.datasciencelabs.mapping.EsperTypeMapping;

public class TypedSelectEsperStreamOperator<KEY, IN extends GenericData.Record, OUT> extends SelectEsperStreamOperator<KEY, IN, OUT> {

	private static final long serialVersionUID = -1443208504064873118L;

	/** Optional type mapping to override the default POJO mapping */
	private final EsperTypeMapping esperTypeMapping;

	/**
	 * Constructs a new operator. Requires the type of the input DataStream to register its Event Type at Esper.
	 * Currently only processing time evaluation is supported.
	 *
	 * @param inputStreamType     type of the input DataStream
	 * @param esperSelectFunction function to select from Esper's output
	 * @param isProcessingTime    Flag indicating how time is interpreted (processing time vs event time)
	 * @param esperQuery          The esper query
	 * @param esperTypeMapping    Type mapping provider to enable non-Pojo event types
	 */
	public TypedSelectEsperStreamOperator(TypeInformation<IN> inputStreamType, EsperSelectFunction<OUT> esperSelectFunction, boolean isProcessingTime, EsperStatementFactory esperQuery, EsperTypeMapping esperTypeMapping) {
		super(inputStreamType, esperSelectFunction, isProcessingTime, esperQuery);
		this.esperTypeMapping = esperTypeMapping;
	}

	@Override
	protected void internalSetupServiceProvider(EPServiceProvider serviceProvider) {
		esperTypeMapping.getEventTypes().forEach((type, schema) -> {
			ConfigurationEventTypeAvro avroEvent = new ConfigurationEventTypeAvro(schema);
			serviceProvider.getEPAdministrator().getConfiguration().addEventTypeAvro(type, avroEvent);
		});
	}

	@Override
	protected void sendEvent(StreamRecord<IN> streamRecord, EPServiceProvider serviceProvider) {
		serviceProvider.getEPRuntime().sendEventAvro(streamRecord.getValue(), streamRecord.getValue().getSchema().getName());
	}
}
