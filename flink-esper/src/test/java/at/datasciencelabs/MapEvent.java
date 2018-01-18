package at.datasciencelabs;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class MapEvent implements Serializable {

	private static final long serialVersionUID = -4656051573542193337L;

	private Map<String, Object> values = new HashMap<>();
	private String type;

	public MapEvent(String type) {
		Objects.requireNonNull(type);
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

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		MapEvent mapEvent = (MapEvent) o;

		if (!values.equals(mapEvent.values)) {
			return false;
		}
		return type.equals(mapEvent.type);
	}

	@Override
	public int hashCode() {
		int result = values.hashCode();
		result = 31 * result + type.hashCode();
		return result;
	}
}
