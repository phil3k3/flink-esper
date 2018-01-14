package at.datasciencelabs;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.time.CurrentTimeEvent;
import com.espertech.esper.client.time.CurrentTimeSpanEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.io.Serializable;

/**
 * An operator which supports detecting event sequences and patterns using Esper.
 *
 * @param <KEY> Type of the key
 * @param <IN>  Type of the input stream
 * @param <OUT> Type of the output stream
 */
public class SelectEsperStreamOperator<KEY, IN, OUT> extends AbstractUdfStreamOperator<OUT, EsperSelectFunction<OUT>> implements OneInputStreamOperator<IN, OUT>, Triggerable<KEY, VoidNamespace>, Serializable {

    private static final String ESPER_SERVICE_PROVIDER_STATE = "esperServiceProviderState";

    /** The Esper query to execute */
    private final EsperStatementFactory query;

    /** The inferred input type of the user function */
    private final TypeInformation<IN> inputType;

    /** The lock for creating a thread-safe instance of an Esper service provider */
    private final Object lock = new Object[0];

    /** The state containing the Esper engine */
    private ValueState<EPServiceProvider> engineState;

    /**
     * Constructs a new operator. Requires the type of the input DataStream to register its Event Type at Esper.
     * Currently only processing time evaluation is supported.
     *
     * @param inputStreamType     type of the input DataStream
     * @param esperSelectFunction function to select from Esper's output
     * @param isProcessingTime    Flag indicating how time is interpreted (processing time vs event time)
     * @param esperQuery          The esper query
     */
    public SelectEsperStreamOperator(TypeInformation<IN> inputStreamType, EsperSelectFunction<OUT> esperSelectFunction, boolean isProcessingTime, EsperStatementFactory esperQuery) {
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

        if (this.engineState == null) {
            this.engineState = getRuntimeContext().getState(new ValueStateDescriptor<>(ESPER_SERVICE_PROVIDER_STATE, new EsperEngineSerializer()));
        }
    }

    @Override
    public void processElement(StreamRecord<IN> streamRecord) throws Exception {
        EPServiceProvider esperServiceProvider = getServiceProvider(this.hashCode() + "");
        esperServiceProvider.getEPRuntime().sendEvent(streamRecord.getValue());
        this.engineState.update(esperServiceProvider);
    }

    @Override
    public void onEventTime(InternalTimer<KEY, VoidNamespace> internalTimer) throws Exception {
        // not supported yet
    }

    @Override
    public void onProcessingTime(InternalTimer<KEY, VoidNamespace> internalTimer) throws Exception {
        EPServiceProvider epServiceProvider = getServiceProvider(this.hashCode() + "");
        epServiceProvider.getEPRuntime().sendEvent(new CurrentTimeSpanEvent(internalTimer.getTimestamp()));
        this.engineState.update(epServiceProvider);
    }

    private EPServiceProvider getServiceProvider(String context) throws IOException {
        EPServiceProvider serviceProvider = engineState.value();
        if (serviceProvider != null) {
            return serviceProvider;
        }
        synchronized (lock) {
            serviceProvider = engineState.value();
            if (serviceProvider == null) {
                Configuration configuration = new Configuration();
                configuration.getEngineDefaults().getThreading().setInternalTimerEnabled(false);
                serviceProvider = EPServiceProviderManager.getProvider(context, configuration);
                serviceProvider.getEPAdministrator().getConfiguration().addEventType(inputType.getTypeClass());
                serviceProvider.getEPRuntime().sendEvent(new CurrentTimeEvent(0));
                EPStatement statement = query.createStatement(serviceProvider.getEPAdministrator());

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
                this.engineState.update(serviceProvider);
                return serviceProvider;

            } else {
                return engineState.value();
            }
        }
    }
}
