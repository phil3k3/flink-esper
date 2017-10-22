package at.datasciencelabs;

import com.espertech.esper.client.EventBean;
import org.apache.flink.api.common.functions.Function;

import java.io.Serializable;

/**
 * Function to transform the EventBean into the corresponding DataStream output object.
 * @param <OUT> Type of the output DataStream
 */
public interface EsperSelectFunction<OUT> extends Function, Serializable {

    /**
     * Select by transforming an {@link EventBean} to an instance of the OUT type.
     * @param eventBean Result event of an esper pattern
     * @return The transformed instance
     * @throws Exception If there is an error in the transformation.
     */
    OUT select(EventBean eventBean) throws Exception;
}
