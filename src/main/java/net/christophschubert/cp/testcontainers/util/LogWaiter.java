package net.christophschubert.cp.testcontainers.util;

public class LogWaiter {
    public boolean found = false;
    String pattern;

    public LogWaiter(String pattern) {
        this.pattern = pattern;
    }

    public void accept(String s) {
        System.out.print(s);
        if (s.contains(pattern))
            found = true;
    }
}
