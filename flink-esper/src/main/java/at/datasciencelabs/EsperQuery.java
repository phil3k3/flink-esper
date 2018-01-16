package at.datasciencelabs;

import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPStatement;

class EsperQuery implements EsperStatementFactory {

    private String query;

    EsperQuery(String query) {
        this.query = query;
    }


    @Override
    public EPStatement createStatement(EPAdministrator administrator) {
        return administrator.createEPL(query);
    }
}
