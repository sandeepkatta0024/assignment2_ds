private static class WeatherRecord {
    JsonObject obj;
    long timestamp;
    int lamport;
    WeatherRecord(JsonObject obj, int lamport) {
        this.obj = obj;
        this.lamport = lamport;
        this.timestamp = System.currentTimeMillis();
    }
}