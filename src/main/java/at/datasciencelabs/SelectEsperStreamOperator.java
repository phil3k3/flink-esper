package at.datasciencelabs;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.Serializable;

/**
 * An operator which supports detecting event sequences and patterns using Esper.
 * @param <KEY> Type of the key
 * @param <IN> Type of the input stream
 * @param <OUT> Type of the output stream
 */
public class SelectEsperStreamOperator<KEY, IN, OUT> extends AbstractUdfStreamOperator<OUT, EsperSelectFunction<OUT>> implements OneInputStreamOperator<IN, OUT>, Triggerable<KEY, VoidNamespace>, Serializable {

    private final String query;
    private final TypeInformation<IN> inputType;
    private EPServiceProvider engine;

    /**
     * Constructs a new operator. Requires the type of the input DataStream to register its Event Type at Esper.
     * Currently only processing time evaluation is supported.
     * @param inputStreamType type of the input DataStream
     * @param esperSelectFunction function to select from Esper's output
     * @param isProcessingTime Flag indicating how time is interpreted (processing time vs event time)
     * @param esperQuery The esper query
     */
    public SelectEsperStreamOperator(TypeInformation<IN> inputStreamType, EsperSelectFunction<OUT> esperSelectFunction, boolean isProcessingTime, String esperQuery) {
        super(esperSelectFunction);
        this.inputType = inputStreamType;
        this.query = esperQuery;

        if (!isProcessingTime) {
            throw new UnsupportedOperationException("Event-time is not supported");
        }
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        engine = EPServiceProviderManager.getDefaultProvider();
        engine.getEPAdministrator().getConfiguration().addEventType(inputType.getTypeClass());
        EPStatement statement = engine.getEPAdministrator().createEPL(query);
        statement.addListener((newData, oldData) -> {
            for (EventBean event : newData) {
                EsperSelectFunction<OUT> userFunction = getUserFunction();
                try {
                    output.collect(new StreamRecord<>((userFunction.select(event))));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    @Override
    public void processElement(StreamRecord<IN> streamRecord) throws Exception {
        engine.getEPRuntime().sendEvent(streamRecord.getValue());
    }

    @Override
    public void onEventTime(InternalTimer<KEY, VoidNamespace> internalTimer) throws Exception {
        internalTimer.getTimestamp();

    }

    @Override
    public void onProcessingTime(InternalTimer<KEY, VoidNamespace> internalTimer) throws Exception {

    }
}
