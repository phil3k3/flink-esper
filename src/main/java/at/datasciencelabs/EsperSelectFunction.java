package at.datasciencelabs;

import com.espertech.esper.client.EventBean;
import org.apache.flink.api.common.functions.Function;

import java.io.Serializable;

public interface EsperSelectFunction<OUT> extends Function, Serializable {
    OUT select(EventBean collector) throws Exception;
}
