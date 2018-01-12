package at.datasciencelabs;

public class BuildFailure implements BuildEvent {

    private String project;
    private int buildId;

    BuildFailure(String project, int buildId) {
        this.project = project;
        this.buildId = buildId;
    }

    public int getBuildId() {
        return buildId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BuildFailure that = (BuildFailure) o;

        if (buildId != that.buildId) return false;
        return project != null ? project.equals(that.project) : that.project == null;
    }

    @Override
    public int hashCode() {
        int result = project != null ? project.hashCode() : 0;
        result = 31 * result + buildId;
        return result;
    }

    public void setBuildId(int buildId) {
        this.buildId = buildId;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

}
