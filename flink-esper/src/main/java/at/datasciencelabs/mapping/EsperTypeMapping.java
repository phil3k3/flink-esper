package at.datasciencelabs.mapping;

import java.io.Serializable;
import java.net.URI;

public interface EsperTypeMapping extends Serializable {
	Class getEventRepresentationClass();
	URI getEventUri();
}
