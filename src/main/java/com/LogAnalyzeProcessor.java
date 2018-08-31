package com;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class LogAnalyzeProcessor {

    private static final DateTimeFormatter hourFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
    private static final DateTimeFormatter dayFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter monthFormatter = DateTimeFormatter.ofPattern("yyyy-MM");

    private final String inputDirectory;
    private final Map<String, Optional<?>> arguments;

    public LogAnalyzeProcessor(String inputDirectory, Map<String, Optional<?>> arguments){
        this.inputDirectory = inputDirectory;
        this.arguments = arguments;
    }

    public void analyze() throws IOException, InterruptedException {
        List<Path> allFilesList = Files.list(Paths.get(inputDirectory)).collect(Collectors.toList());
        int threadsCount = (Integer)arguments.get("threads").get();
        List<Path>[] filesPerThread = split(allFilesList, threadsCount);
        Queue<String> normalized = null;
        ExecutorService executor = Executors.newFixedThreadPool(threadsCount);

        Map container = null;
        EnumSet<GroupingOptions> groupingOptions = null;
        if(arguments.get("Gusername").isPresent() && arguments.get("Gtime").isPresent()){
            groupingOptions = GroupingOptions.ALL;
            container = new HashMap<String, HashMap<LocalDateTime, Long>>();
        } else if(arguments.get("Gusername").isPresent()){
            groupingOptions = EnumSet.of(GroupingOptions.USERNAME);
            container = new HashMap<String, Long>();
        } else if(arguments.get("Gtime").isPresent()){
            groupingOptions = EnumSet.of(GroupingOptions.TIMEUNIT);
            container = new HashMap<LocalDateTime, Long>();
        }
        for (List<Path> files: filesPerThread) {
            executor.execute(new Thread(new LogAnalyzer<>(files, arguments, container, groupingOptions)));
        }
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        normalized = normalizeOutputGroupedByUsernameAndTimePeriod(container, (ChronoUnit)arguments.get("Gtime").get());

        if(arguments.get("output").isPresent()){
            Files.write(Paths.get((String) arguments.get("output").get()), normalized);
        } else {
            Files.write(Paths.get("output.txt"), normalized);
        }
    }

    private static Queue<String> normalizeOutputGroupedByUsernameAndTimePeriod(Map<String, HashMap<LocalDateTime, Long>> container, ChronoUnit chronoUnit){
        Queue<String> normalized = new LinkedList<>();
        container.forEach((user, periodRecordsCount) -> {
            normalized.add(user);
            switch(chronoUnit){
                case HOURS: {
                    normalized.add("Hour                Count of records");
                    periodRecordsCount.entrySet().stream()
                            .sorted(Map.Entry.comparingByKey())
                            .forEachOrdered(localDateTimeLongEntry -> {
                                normalized.add(localDateTimeLongEntry.getKey().format(hourFormatter) + "    " + localDateTimeLongEntry.getValue());
                            });
                } ; break;
                case DAYS: {
                    normalized.add("Day                 Count of records");
                    periodRecordsCount.entrySet().stream()
                            .sorted(Map.Entry.comparingByKey())
                            .forEachOrdered(localDateTimeLongEntry -> {
                                normalized.add(localDateTimeLongEntry.getKey().format(dayFormatter) + "          " + localDateTimeLongEntry.getValue());
                            });
                } ; break;
                case MONTHS: {
                    normalized.add("Month               Count of records");
                    periodRecordsCount.entrySet().stream()
                            .sorted(Map.Entry.comparingByKey())
                            .forEachOrdered(localDateTimeLongEntry -> {
                                normalized.add(localDateTimeLongEntry.getKey().format(monthFormatter) + "             " + localDateTimeLongEntry.getValue());
                            });
                } ; break;
            }
        });
        return normalized;
    }

    private static Queue<String> normalizeOutputGroupedByUsername(Map<String, Long> container){
        Queue<String> normalized = new LinkedList<>();
        normalized.add("Username            Count of records");
        container.forEach((user, recordsCount) -> normalized.add(String.format("%-20s", user) + recordsCount));
        return normalized;
    }

    private static Queue<String> normalizeOutputGroupedByTimePeriod(Map<LocalDateTime, Long> container, ChronoUnit chronoUnit){
        Queue<String> normalized = new LinkedList<>();
        switch(chronoUnit){
            case HOURS: {
                normalized.add("Hour                Count of records");
                container.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .forEachOrdered(localDateTimeLongEntry -> {
                            normalized.add(localDateTimeLongEntry.getKey().format(hourFormatter) + "    " + localDateTimeLongEntry.getValue());
                        });
            }; break;
            case DAYS: {
                normalized.add("Day                 Count of records");
                container.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .forEachOrdered(localDateTimeLongEntry -> {
                            normalized.add(localDateTimeLongEntry.getKey().format(dayFormatter) + "          " + localDateTimeLongEntry.getValue());
                        });
            } ; break;
            case MONTHS: {
                normalized.add("Month               Count of records");
                container.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .forEachOrdered(localDateTimeLongEntry -> {
                            normalized.add(localDateTimeLongEntry.getKey().format(monthFormatter) + "             " + localDateTimeLongEntry.getValue());
                        });
            } ; break;
        }
        return normalized;
    }

    private static List[] split(List<Path> list, int numberOfParts){
        List[] parts = new List[numberOfParts];
        int partSize = list.size() / numberOfParts;
        int i = 0;
        for( ; i < numberOfParts - 1 ; i++){
            parts[i] = list.subList(i * partSize, i * partSize + partSize);
        }
        parts[i] = list.subList(i * numberOfParts, list.size());
        return parts;
    }
}
