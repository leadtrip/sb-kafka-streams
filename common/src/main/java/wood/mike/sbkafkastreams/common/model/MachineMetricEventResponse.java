package wood.mike.sbkafkastreams.common.model;

public record MachineMetricEventResponse(String message) {
    public static MachineMetricEventResponse of(String message) {return  new MachineMetricEventResponse(message);}
}
