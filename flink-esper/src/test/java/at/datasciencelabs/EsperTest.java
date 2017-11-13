package at.datasciencelabs;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import org.junit.Test;

public class EsperTest {

    @Test
    public void testEsper() {
        EPServiceProvider engine = EPServiceProviderManager.getDefaultProvider();

        engine.getEPAdministrator().getConfiguration().addEventType(TestEvent.class);

        String query = "select name, age from TestEvent";

        EPStatement statement = engine.getEPAdministrator().createEPL(query);

        statement.addListener((newData, oldData) -> {
            String name = (String) newData[0].get("name");
            int age = (int) newData[0].get("age");
            System.out.println(name + " " + age);
        });

        engine.getEPRuntime().sendEvent(new TestEvent("peter", 10));
    }
}
