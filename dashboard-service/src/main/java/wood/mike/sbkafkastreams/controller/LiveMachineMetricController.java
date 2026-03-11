package wood.mike.sbkafkastreams.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class LiveMachineMetricController {

    @GetMapping("/live-machine-metrics")
    public String liveMonitor() {
        return "live-machine-metrics";
    }
}

