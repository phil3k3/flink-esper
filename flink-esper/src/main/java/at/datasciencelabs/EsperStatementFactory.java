package at.datasciencelabs;

import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPStatement;

import java.io.Serializable;

interface EsperStatementFactory extends Serializable {
    EPStatement createStatement(EPAdministrator administrator);
}
