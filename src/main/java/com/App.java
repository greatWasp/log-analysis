package com;

import org.apache.commons.cli.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class App
{
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter hourFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
    private static final DateTimeFormatter dayFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter monthFormatter = DateTimeFormatter.ofPattern("yyyy-MM");
    private static final Pattern timeUnitPattern = Pattern.compile("1\\s(hour|month|day)");

    private static final String[] appArgs = {"username", "since", "until", "message", "Gusername", "Gtime", "threads", "output"};

    public static void main( String[] args ) throws IOException, InterruptedException {
        Options options = new Options();

        CustomOptionGroup filteringGroup = new CustomOptionGroup();
        filteringGroup.setRequired(true);

        CustomOption usernameOption = new CustomOption("username", true, "Filters records by username");
        usernameOption.setArgName("value");
        filteringGroup.addOption(usernameOption);

        CustomOption sinceOption = new CustomOption("since", true, "Filters records that are more recent than a specific date. date format is yyyy-MM-dd HH:mm:ss");
        sinceOption.setArgName("date");
        filteringGroup.addOption(sinceOption);

        CustomOption untilOption = new CustomOption("until", true, "Filters records that are older than a specific date. date format is yyyy-MM-dd HH:mm:ss");
        untilOption.setArgName("date");
        filteringGroup.addOption(untilOption);

        CustomOption messageOption = new CustomOption("message", true, "Filters records by message matching specified pattern (regular expression)");
        messageOption.setArgName("pattern");
        filteringGroup.addOption(messageOption);

        CustomOptionGroup groupingGroup = new CustomOptionGroup();
        groupingGroup.setRequired(true);

        CustomOption gUsernameOption = new CustomOption("Gusername", false, "Groups records by username");
        groupingGroup.addOption(gUsernameOption);

        CustomOption gTimeOption = new CustomOption("Gtime", true, "Groups records by period of time. Valid periods of time are: [1 hour | 1 day | 1 month]");
        gTimeOption.setArgName("unit");
        groupingGroup.addOption(gTimeOption);

        options.addOptionGroup(filteringGroup);
        options.addOptionGroup(groupingGroup);

        Option threadsOption = new Option("threads", true, "Use <n> number of threads");
        threadsOption.setArgName("n");
        options.addOption(threadsOption);

        Option outputOption = new Option("output", true, "Path to output file");
        outputOption.setArgName("path");
        options.addOption(outputOption);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter helpFormatter = new HelpFormatter();

        try {
            CommandLine cmd = parser.parse(options, args);

            Optional<?>[] arguments = parseArguments(cmd);
            List<Path> allFilesList = Files.list(Paths.get(".")).collect(Collectors.toList());
            int threadsCount = (Integer)arguments[6].get();
            long filesPerThread = allFilesList.size() / threadsCount;
            filesPerThread = filesPerThread == 0 ? 1 : filesPerThread;
            Stream<Path>[] fileStreamsPerThread = new Stream[threadsCount];
            int j = 0;
            for( ; j < threadsCount - 1 ; j++){
                fileStreamsPerThread[j] = allFilesList.subList((int)( j * filesPerThread), (int)(j * filesPerThread + filesPerThread)).stream();
            }
            fileStreamsPerThread[j] = allFilesList.subList((int) (j * filesPerThread), allFilesList.size()).stream();
            List<String> normalized = new ArrayList<>();
            Thread[] threads = new Thread[threadsCount];
            if(arguments[4].isPresent() && arguments[5].isPresent()){
                HashMap<String, HashMap<LocalDateTime, Long>> container = new HashMap<>();
                for(int i = 0 ; i < threadsCount; i++) {
                    Thread thread = new Thread(new LogAnalyzer<>(fileStreamsPerThread[i], arguments, container));
                    threads[i] = thread;
                    thread.start();
                }
                for(int i = 0 ; i < threadsCount; i++) {
                    threads[i].join();
                }
                normalized = normalizeOutputGroupedByUsernameAndTimePeriod(container, (ChronoUnit)arguments[5].get());
            } else if(arguments[4].isPresent()){
                HashMap<String, Long> container = new HashMap<>();
                for(int i = 0 ; i < threadsCount; i++){
                    Thread thread = new Thread(new LogAnalyzer<>(fileStreamsPerThread[i], arguments, container));
                    threads[i] = thread;
                    thread.start();
                }
                for(int i = 0 ; i < threadsCount; i++) {
                    threads[i].join();
                }
                normalized = normalizeOutputGroupedByUsername(container);
            } else if(arguments[5].isPresent()){
                HashMap<LocalDateTime, Long> container = new HashMap<>();
                for(int i = 0 ; i < threadsCount; i++){
                    Thread thread = new Thread(new LogAnalyzer<>(fileStreamsPerThread[i], arguments, container));
                    threads[i] = thread;
                    thread.start();
                }
                for(int i = 0 ; i < threadsCount; i++) {
                    threads[i].join();
                }
                normalized = normalizeOutputGroupedByTimePeriod(container, (ChronoUnit)arguments[5].get());
            }

            if(arguments[7].isPresent()){
                Files.write(Paths.get((String) arguments[7].get()), normalized);
            } else {
                Files.write(Paths.get("output.txt"), normalized);
            }
        } catch (ParseException | DateTimeParseException | PatternSyntaxException e) {
            System.out.println(e.getMessage());
            helpFormatter.printHelp("log-analysis", options);
            System.exit(1);
        }
    }

    private static List<String> normalizeOutputGroupedByUsernameAndTimePeriod(Map<String, HashMap<LocalDateTime, Long>> container, ChronoUnit chronoUnit){
        List<String> normalized = new LinkedList<>();
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

    private static List<String> normalizeOutputGroupedByUsername(Map<String, Long> container){
        List<String> normalized = new LinkedList<>();
        normalized.add("Username            Count of records");
        container.forEach((user, recordsCount) -> normalized.add(String.format("%-20s", user) + recordsCount));
        return normalized;
    }

    private static List<String> normalizeOutputGroupedByTimePeriod(Map<LocalDateTime, Long> container, ChronoUnit chronoUnit){
        List<String> normalized = new LinkedList<>();
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

    private static Optional<?>[] parseArguments(CommandLine cmd){
        Optional<?>[] result = new Optional[appArgs.length];
        Optional<String> usernameArgument = Optional.ofNullable(cmd.getOptionValue("username"));
        Optional<String> sinceArgument = Optional.ofNullable(cmd.getOptionValue("since"));
        Optional<String> untilArgument = Optional.ofNullable(cmd.getOptionValue("until"));
        Optional<String> messageArgument = Optional.ofNullable(cmd.getOptionValue("message"));
        Optional<Boolean> gUsernameArgument = Optional.ofNullable(cmd.hasOption("Gusername") ? true : null );
        Optional<String> gTimeArgument = Optional.ofNullable(cmd.getOptionValue("Gtime"));
        Optional<String> threadsArgument = Optional.ofNullable(cmd.getOptionValue("threads"));
        Optional<String> outputArgument = Optional.ofNullable(cmd.getOptionValue("output"));

        Optional<LocalDateTime> sinceDate = Optional.empty();
        Optional<LocalDateTime> untilDate = Optional.empty();
        Optional<Pattern> messagePattern = Optional.empty();
        Optional<ChronoUnit> gTimePeriod = Optional.empty();
        Optional<Integer> threads;

        if (sinceArgument.isPresent()){
            try{
                sinceDate = Optional.of(LocalDateTime.parse(sinceArgument.get(), dateTimeFormatter));
            } catch (DateTimeParseException e){
                sinceDate = Optional.of(LocalDate.parse(sinceArgument.get(), dateFormatter).atStartOfDay());
            }
        }
        if (untilArgument.isPresent()){
            try{
                untilDate = Optional.of(LocalDateTime.parse(untilArgument.get(), dateTimeFormatter));
            } catch (DateTimeParseException e){
                untilDate = Optional.of(LocalDate.parse(untilArgument.get(), dateFormatter).atStartOfDay());
            }
        }
        if (messageArgument.isPresent()){
            messagePattern = Optional.of(Pattern.compile(messageArgument.get()));
        }
        if (gTimeArgument.isPresent()){
            Matcher matcher = timeUnitPattern.matcher(gTimeArgument.get());
            if(matcher.find() && matcher.start() == 0 && matcher.end() == gTimeArgument.get().length()){
                if(matcher.group(1).startsWith("hour")){
                    gTimePeriod = Optional.of(ChronoUnit.HOURS);
                }
                if(matcher.group(1).startsWith("day")){
                    gTimePeriod = Optional.of(ChronoUnit.DAYS);
                }
                if(matcher.group(1).startsWith("month")){
                    gTimePeriod = Optional.of(ChronoUnit.MONTHS);
                }
            }
        }
        int threadsNumber = Integer.parseInt(threadsArgument.orElse("1"));
        if (threadsNumber <= 0){
            threadsNumber = 1;
        }
        threads = Optional.of(threadsNumber);

        result[0] = usernameArgument;
        result[1] = sinceDate;
        result[2] = untilDate;
        result[3] = messagePattern;
        result[4] = gUsernameArgument;
        result[5] = gTimePeriod;
        result[6] = threads;
        result[7] = outputArgument;
        return result;
    }

    private static void printHelp(){
        System.out.println("usage: log-analysis [<parameters>]");
        System.out.println();
        System.out.println("Parameters are:");
        System.out.println("filtering (at least one parameter should be specified):");
        System.out.println("\t-username <value>   Filters records by username");
        System.out.println("\t-since <date>       Filters records that are more recent than a specific date");
        System.out.println("\t-until <date>       Filters records that are older than a specific date");
        System.out.println("\t-message <pattern>  Filters records by message matching specified pattern (regular expression)");
        System.out.println();
        System.out.println("grouping (at least one parameter should be specified):");
        System.out.println("\t-Gusername          Groups records by username");
        System.out.println("\t-Gtime <unit>       Groups records by period of time");
        System.out.println();
        System.out.println("processing:");
        System.out.println("\t-threads <n>        Use <n> number of threads");
        System.out.println("\t-output <path>      Path to output file");
    }
}
