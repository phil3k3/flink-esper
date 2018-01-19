package at.datasciencelabs;

import java.io.Serializable;
import java.util.Map;
import org.apache.avro.Schema;

/**
 * Provides schema information to Esper.
 */
public interface SchemaProvider extends Serializable {

	/**
	 * Returns the available event types and their associated schemas.
	 */
	Map<String, Schema> getEventTypes();
}
