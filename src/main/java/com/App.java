package com;

import org.apache.commons.cli.*;

import java.io.IOException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class App
{
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
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

            Map<String, Optional<?>> arguments = parseArguments(cmd);
            LogAnalyzeProcessor processor = new LogAnalyzeProcessor(".", arguments);
            processor.analyze();
        } catch (ParseException | DateTimeParseException | PatternSyntaxException e) {
            System.out.println(e.getMessage());
            helpFormatter.printHelp("log-analysis", options);
            System.exit(1);
        }
    }

    private static Map<String, Optional<?>> parseArguments(CommandLine cmd){
        Map<String, Optional<?>> result = new HashMap<>();
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

        result.put("username", usernameArgument);
        result.put("since", sinceDate);
        result.put("until", untilDate);
        result.put("message", messagePattern);
        result.put("Gusername", gUsernameArgument);
        result.put("Gtime", gTimePeriod);
        result.put("threads", threads);
        result.put("output", outputArgument);
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
