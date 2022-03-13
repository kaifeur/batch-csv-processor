package nyu.rubtsov.batch.csv.configuration;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import nyu.rubtsov.batch.csv.conversion.DateConversionService;
import nyu.rubtsov.batch.csv.field.extractor.DateFormattingFieldExtractor;
import nyu.rubtsov.batch.csv.model.Person;
import nyu.rubtsov.batch.csv.partitioner.ZipToCsvPartitioner;
import nyu.rubtsov.batch.csv.reader.PersonReader;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.core.task.SyncTaskExecutor;

@Configuration
@EnableConfigurationProperties(InputDate.class)
@EnableBatchProcessing
public class BatchConfiguration extends DefaultBatchConfigurer {

    public static final String[] FIELD_NAMES = {"firstName", "lastName", "date"};

    @Bean
    public Job zipCsvReadingJob(
            JobBuilderFactory jobBuilderFactory,
            @Value("${job.name:zipToCsv}") String jobName,
            Step partitionerStep
    ) {
        return jobBuilderFactory.get(jobName)
                .incrementer(new RunIdIncrementer())
                .flow(partitionerStep)
                .end()
                .build();
    }

    @Bean
    public ZipToCsvPartitioner partitioner(
            @Value("${input.file}") String inputFilePath,
            ResourcePatternResolver resourcePatternResolver
    ) throws IOException {
        return new ZipToCsvPartitioner(inputFilePath, resourcePatternResolver);
    }

    @Bean
    public Step partitionerStep(
            StepBuilderFactory stepBuilderFactory,
            @Value("${step.partitioner.name:partitionerStep}") String stepName,
            Partitioner partitioner,
            Step mainStep
    ) {
        return stepBuilderFactory.get(stepName)
                .partitioner(stepName, partitioner)
                .gridSize(1)
                .taskExecutor(new SyncTaskExecutor())
                .step(mainStep)
                .build();
    }

    @Bean
    public Step mainStep(
            StepBuilderFactory stepBuilderFactory,
            @Value("${step.main.name:mainStep}") String stepName,
            ItemReader<Person> itemReader,
            ItemWriter<Person> itemWriter
    ) {
        return stepBuilderFactory.get(stepName)
                .<Person, Person>chunk(128)
                .reader(itemReader)
                .writer(itemWriter)
                .build();
    }

    @Bean
    public DateConversionService dateConversionService(InputDate inputDate) {
        List<SimpleDateFormat> formats = inputDate.getPatterns().stream()
                .map(p -> new SimpleDateFormat(p, Locale.ENGLISH))
                .collect(Collectors.toList());

        return new DateConversionService(formats);
    }

    @Bean
    public ItemReader<Person> itemReader(
            ZipToCsvPartitioner partitioner,
            @Value("${item.reader.name:personReader}") String readerName,
            DateConversionService dateConversionService
    ) {
        PersonReader personReader = new PersonReader(partitioner.getFileSystem());
        personReader.setName(readerName);
        personReader.setLinesToSkip(1); // header

        DefaultLineMapper<Person> lineMapper = new DefaultLineMapper<>();
        personReader.setLineMapper(lineMapper);

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setNames(FIELD_NAMES);
        lineMapper.setLineTokenizer(lineTokenizer);

        BeanWrapperFieldSetMapper<Person> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Person.class);
        fieldSetMapper.setConversionService(dateConversionService);

        lineMapper.setFieldSetMapper(fieldSetMapper);

        return personReader;
    }

    @Bean
    public ItemWriter<Person> itemWriter(
            @Value("${item.writer.name:personWriter") String writerName,
            @Value("${output.file}") String outputFilePath,
            @Value("${output.date.pattern:dd/MM/yyyy}") String outputDatePattern
    ) throws IOException {
        BeanWrapperFieldExtractor<Person> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(FIELD_NAMES);

        SimpleDateFormat format = new SimpleDateFormat(outputDatePattern, Locale.ENGLISH);
        DateFormattingFieldExtractor<Person> dateFormattingFieldExtractor = new DateFormattingFieldExtractor<>(format, fieldExtractor);

        DelimitedLineAggregator<Person> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");
        lineAggregator.setFieldExtractor(dateFormattingFieldExtractor);

        return new FlatFileItemWriterBuilder<Person>()
                .name(writerName)
                .resource(new FileSystemResource(outputFilePath))
                .delimited()
                .delimiter(",")
                .fieldExtractor(dateFormattingFieldExtractor)
                .append(true)
                .headerCallback(writer -> {
                    for (int i = 0; i < FIELD_NAMES.length - 1; i++) {
                        writer.write(FIELD_NAMES[i] + ",");
                    }
                    writer.write(FIELD_NAMES[FIELD_NAMES.length - 1]);
                })
                .build();
    }
}
