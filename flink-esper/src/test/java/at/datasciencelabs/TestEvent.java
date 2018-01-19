package at.datasciencelabs;

import java.util.Objects;

public class TestEvent {

    private String name;
    private int age;

    TestEvent(String name, int age) {
        Objects.requireNonNull(name);
        this.name = name;
        this.age = age;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TestEvent testEvent = (TestEvent) o;

        return age == testEvent.age && name.equals(testEvent.name);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + age;
        return result;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    @Override
    public String toString() {
        return "TestEvent{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
