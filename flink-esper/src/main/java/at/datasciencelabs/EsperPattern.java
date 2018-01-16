package at.datasciencelabs;

import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPStatement;

class EsperPattern implements EsperStatementFactory {

    private String pattern;

    EsperPattern(String pattern) {
        this.pattern = pattern;
    }

    @Override
    public EPStatement createStatement(EPAdministrator administrator) {
        return administrator.createPattern(pattern);
    }
}
