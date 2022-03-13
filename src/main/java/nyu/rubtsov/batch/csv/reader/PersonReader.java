package nyu.rubtsov.batch.csv.reader;

import java.nio.file.FileSystem;
import java.nio.file.Path;
import nyu.rubtsov.batch.csv.model.Person;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.core.io.PathResource;

import static nyu.rubtsov.batch.csv.partitioner.ZipToCsvPartitioner.CONTEXT_KEY;

public class PersonReader extends FlatFileItemReader<Person> implements StepExecutionListener {

    private final FileSystem fileSystem;

    public PersonReader(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        ExecutionContext executionContext = stepExecution.getExecutionContext();
        String pathStr = (String) executionContext.get(CONTEXT_KEY);
        if (pathStr == null) {
            throw new IllegalArgumentException("Execution context does not contain input by key " + CONTEXT_KEY);
        }

        Path path = fileSystem.getPath(pathStr);

        this.setResource(new PathResource(path));
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        return stepExecution.getExitStatus();
    }
}
