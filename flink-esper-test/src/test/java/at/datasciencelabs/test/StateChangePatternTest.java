package at.datasciencelabs.test;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.event.map.MapEventBean;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StateChangePatternTest {

    private EPServiceProvider engine;

    private static final String STATE_CHANGE_PATTERN = "every(A=BuildSuccess) -> (B=BuildFailure(project=A.project) and not A=BuildSuccess)";

    private EventBuilder eventsBuilder;

    @Before
    public void onBefore() {
        engine = EPServiceProviderManager.getDefaultProvider();

        engine.getEPAdministrator().getConfiguration().addEventType(BuildSuccess.class);
        engine.getEPAdministrator().getConfiguration().addEventType(BuildFailure.class);

        eventsBuilder = new EventBuilder();
    }

    @Test
    public void patternDetected() {
        List<BuildEvent> buildEvents = Lists.newArrayList(
                eventsBuilder.expectedBuildSuccess(),
                eventsBuilder.expectedBuildFailure()
        );

        runWithPatternAndEvents(STATE_CHANGE_PATTERN, buildEvents);
    }

    @Test
    public void precedingBuildSuccessIgnored() {
        List<BuildEvent> buildEvents = Lists.newArrayList(
                eventsBuilder.buildSuccess(),
                eventsBuilder.expectedBuildSuccess(),
                eventsBuilder.expectedBuildFailure()
        );

        runWithPatternAndEvents(STATE_CHANGE_PATTERN, buildEvents);
    }

    @Test
    public void patternDetectedTwice() {

        List<BuildEvent> buildEvents = Lists.newArrayList(
                eventsBuilder.expectedBuildSuccess(),
                eventsBuilder.expectedBuildFailure(),
                eventsBuilder.expectedBuildSuccess(),
                eventsBuilder.expectedBuildFailure()
        );

        runWithPatternAndEvents(STATE_CHANGE_PATTERN, buildEvents);
    }

    @Test
    public void eventsSeparatedByProject() {

        List<BuildEvent> buildEvents = Lists.newArrayList(
                eventsBuilder.buildSuccess(),
                eventsBuilder.expectedBuildSuccess("project2"),
                eventsBuilder.expectedBuildFailure("project2"),
                eventsBuilder.expectedBuildSuccess(),
                eventsBuilder.expectedBuildFailure()
        );

        runWithPatternAndEvents(STATE_CHANGE_PATTERN, buildEvents);
    }

    @Test
    @Ignore("State change pattern does not detect initial failures yet")
    public void singleBuildFailureDetected() {
        List<BuildEvent> buildEvents = Lists.newArrayList(
                eventsBuilder.expectedBuildFailure()
        );
        runWithPatternAndEvents(STATE_CHANGE_PATTERN, buildEvents);
    }

    @Test
    @Ignore("State change pattern does not detect initial failures yet")
    public void precedingBuildFailureDetected() {

        List<BuildEvent> buildEvents = Lists.newArrayList(
                eventsBuilder.expectedBuildFailure(),
                eventsBuilder.expectedBuildSuccess(),
                eventsBuilder.expectedBuildFailure()
        );

        runWithPatternAndEvents(STATE_CHANGE_PATTERN, buildEvents);
    }

    private void runWithPatternAndEvents(String pattern, List<BuildEvent> buildEvents) {
        EPStatement epStatement = engine.getEPAdministrator().createPattern(pattern);

        Map<String, List<BuildEvent>> actualBuildEvents = Maps.newHashMap();
        epStatement.addListener((newData, oldData) -> {
            Joiner.MapJoiner joiner = Joiner.on(",").withKeyValueSeparator("=");
            Lists.newArrayList(newData).forEach(___ -> System.out.println(joiner.join(((MapEventBean) ___).getProperties())));
            Lists.newArrayList(newData).forEach(___ -> ((MapEventBean) ___).getProperties().forEach((key, value) -> {
                actualBuildEvents.putIfAbsent(key, new ArrayList<>());
                actualBuildEvents.get(key).add((BuildEvent) ((EventBean) value).getUnderlying());
            }));
        });

        buildEvents.forEach(___ -> engine.getEPRuntime().sendEvent(___));

        Map<String, List<BuildEvent>> expectedBuildEvents = Maps.newHashMap();
        for (BuildEvent buildEvent : buildEvents) {
            if (buildEvent instanceof Expected) {
                String key = ((Expected) buildEvent).getKey();
                expectedBuildEvents.putIfAbsent(key, new ArrayList<>());
                expectedBuildEvents.get(key).add(buildEvent);
            }
        }

        Assert.assertEquals(expectedBuildEvents, actualBuildEvents);
    }

    public class ExpectedBuildFailure extends BuildFailure implements Expected {

        private final String key;

        ExpectedBuildFailure(String project, int buildId, String key) {
            super(project, buildId);
            this.key = key;
        }

        @Override
        public String getKey() {
            return key;
        }
    }

    public class ExpectedBuildSuccess extends BuildSuccess implements Expected {
        private String key;

        ExpectedBuildSuccess(String project, int buildId, String key) {
            super(project, buildId);
            this.key = key;
        }

        @Override
        public String getKey() {
            return key;
        }
    }

    private class EventBuilder {
        private int buildId = 1;

        BuildFailure expectedBuildFailure(String project) {
            return new ExpectedBuildFailure(project, buildId++, "B");
        }

        BuildSuccess expectedBuildSuccess(String project) {
            return new ExpectedBuildSuccess(project, buildId++, "A");
        }

        BuildSuccess expectedBuildSuccess() {
            return expectedBuildSuccess("project1");
        }

        BuildFailure expectedBuildFailure() {
            return expectedBuildFailure("project1");
        }

        BuildSuccess buildSuccess() {
            return new BuildSuccess("project1", buildId++);
        }
    }

    @SuppressWarnings("WeakerAccess")
    public interface Expected {
        String getKey();
    }
}
