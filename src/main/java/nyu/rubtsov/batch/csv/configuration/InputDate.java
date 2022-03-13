package nyu.rubtsov.batch.csv.configuration;

import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "input.date")
public class InputDate {

    private List<String> patterns;

    public InputDate() {
    }

    public InputDate(List<String> patterns) {
        this.patterns = patterns;
    }

    public List<String> getPatterns() {
        return patterns;
    }

    public void setPatterns(List<String> patterns) {
        this.patterns = patterns;
    }
}
