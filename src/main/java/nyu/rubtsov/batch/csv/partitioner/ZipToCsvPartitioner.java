package nyu.rubtsov.batch.csv.partitioner;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternResolver;

public class ZipToCsvPartitioner implements Partitioner, Closeable {

    private static final String CSV_FILE_EXTENSION = ".csv";
    public static final String CONTEXT_KEY = "file";

    private final FileSystem fileSystem;

    public ZipToCsvPartitioner(
            String inputFilePath,
            ResourcePatternResolver resourcePatternResolver
    ) throws IOException {
        Resource resource = resourcePatternResolver.getResource(inputFilePath);
        Path path = Paths.get(resource.getURI());
        this.fileSystem = FileSystems.newFileSystem(path);
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Iterable<Path> rootDirectories = fileSystem.getRootDirectories();
        Stream<Path> zipEntriesStream = StreamSupport.stream(rootDirectories.spliterator(), false);

        Map<String, ExecutionContext> queue = zipEntriesStream.flatMap(root -> {
                    try {
                        return Files.walk(root);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).filter(Files::isRegularFile)
                .filter(path -> path.getFileName().toString().endsWith(CSV_FILE_EXTENSION))
                .collect(Collectors.toMap(
                                path -> path.toAbsolutePath().toString(),
                                path -> new ExecutionContext(
                                        Map.of(CONTEXT_KEY, path.toAbsolutePath().toString())
                                )
                        )
                );

        return queue;
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    @Override
    public void close() throws IOException {
        fileSystem.close();
    }
}
