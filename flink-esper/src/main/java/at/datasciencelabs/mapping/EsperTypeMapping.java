package at.datasciencelabs.mapping;

import java.io.Serializable;
import java.util.Map;
import org.apache.avro.Schema;

public interface EsperTypeMapping extends Serializable {
	Map<String, Schema> getEventTypes();
}
