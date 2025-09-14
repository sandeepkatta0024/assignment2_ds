public class LamportClock {
    private int time = 0;

    public synchronized int getTime() { return time; }

    public synchronized void tick() { time++; }

    public synchronized void update(int received) {
        time = Math.max(time, received) + 1;
    }
}
