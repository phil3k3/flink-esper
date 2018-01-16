package at.datasciencelabs;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class MapEvent implements Serializable {

	private static final long serialVersionUID = -4656051573542193337L;

	private Map<String, Object> values = new HashMap<>();
	private String type;

	public MapEvent(String type) {
		this.type = type;
	}

	public MapEvent() {

	}

	public Map<String, Object> getValues() {
		return values;
	}

	public void setValues(Map<String, Object> values) {
		this.values = values;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void put(String buildId, Object value) {
		this.values.put(buildId, value);
	}
}
