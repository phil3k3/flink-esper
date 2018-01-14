package at.datasciencelabs.test;

public class BuildSuccess implements BuildEvent {

    private String project;
    private int buildId;

    BuildSuccess(String project, int buildId) {
        this.project = project;
        this.buildId = buildId;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }


    public int getBuildId() {
        return buildId;
    }

    public void setBuildId(int buildId) {
        this.buildId = buildId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BuildSuccess that = (BuildSuccess) o;

        if (buildId != that.buildId) return false;
        return project != null ? project.equals(that.project) : that.project == null;
    }

    @Override
    public int hashCode() {
        int result = project != null ? project.hashCode() : 0;
        result = 31 * result + buildId;
        return result;
    }
}
