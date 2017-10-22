package at.datasciencelabs;

import com.espertech.esper.client.EventBean;
import org.apache.flink.api.common.functions.Function;

import java.io.Serializable;

/**
 * Function to transform the EventBean into the corresponding DataStream output object.
 * @param <OUT> Type of the output DataStream
 */
public interface EsperSelectFunction<OUT> extends Function, Serializable {
    OUT select(EventBean eventBean) throws Exception;
}
