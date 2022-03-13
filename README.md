# CSV processor using Spring Batch

1. Reads zip file, containing CSVs with format `firstName,lastName,date`.
2. Reads every CSV and maps it to `Person` model converting Date supporting different patterns, configurable
   in `application.yaml`.
3. Then dumps all data to single merged CSV with formatted dates (also configurable).

## How to run

```shell
gradle clean bootRun
```

By default, output will be saved to `./merged.csv`.