package com;

import javafx.util.Pair;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LogAnalyzer<T extends Map> implements Runnable {

    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final Pattern logEntryPattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2})\\s\\[(\\w+)\\]\\s:\\s(.*)");

    private final List<Path> files;
    private final Map<String, Optional<?>> arguments;
    private final T container;
    private final EnumSet<GroupingOptions> groupingOptions;

    LogAnalyzer(List<Path> files, Map<String, Optional<?>> arguments, T container, EnumSet<GroupingOptions> groupingOptions){
        this.files = files;
        this.arguments = arguments;
        this.container = container;
        this.groupingOptions = groupingOptions;
    }

    @Override
    public void run() {
        List<LogEntry> logEntries = loadRecords(this.files);

        handleFiltering(logEntries);
        handleGrouping(logEntries);
    }

    private void handleGrouping(List<LogEntry> logEntries){
        if(logEntries.size() == 0){
            return;
        }
        if(groupingOptions.contains(GroupingOptions.TIMEUNIT)){
            LocalDateTime minDate = logEntries.stream().min(Comparator.comparing(LogEntry::getDateTime)).get().getDateTime();
            LocalDateTime maxDate = logEntries.stream().max(Comparator.comparing(LogEntry::getDateTime)).get().getDateTime();
            if(groupingOptions.contains(GroupingOptions.USERNAME)){
                groupByUsernameAndDate(logEntries, this.container, minDate, maxDate);
            } else {
                groupByDate(logEntries, this.container, minDate, maxDate);
            }
        } else if(groupingOptions.contains(GroupingOptions.USERNAME)){
            groupByUsername(logEntries, this.container);
        }
    }

    private void handleFiltering(List<LogEntry> logEntries){
        arguments.get("username").ifPresent(value -> filterByUsername(logEntries, (String)value));
        arguments.get("since").ifPresent(value -> filterByDateSince(logEntries, (LocalDateTime)value));
        arguments.get("until").ifPresent(value -> filterByDateUntil(logEntries, (LocalDateTime)value));
        arguments.get("message").ifPresent(value -> filterByMessage(logEntries, (Pattern)value));
    }

    private void groupByUsernameAndDate(List<LogEntry> logEntries, T container, LocalDateTime minDate, LocalDateTime maxDate) {
        HashMap<String, HashMap<Pair<LocalDateTime, LocalDateTime>, Long>> usernamePeriodRecordsCount = (HashMap) container;
        Map<String, List<LogEntry>> groupedByUsername = logEntries.stream()
                .collect(Collectors.groupingBy(LogEntry::getUsername));
        groupedByUsername.forEach((key, value) -> {
            usernamePeriodRecordsCount.putIfAbsent(key, new HashMap<>());
            groupByDate(value, (T) usernamePeriodRecordsCount.get(key), minDate, maxDate);
        });
    }

    private void groupByDate(List<LogEntry> logEntries, T container, LocalDateTime startDate, LocalDateTime maxDate) {
        HashMap<LocalDateTime, Long> periodRecordsCount = (HashMap) container;
        switch((ChronoUnit)arguments.get("Gtime").get()){
            case HOURS: startDate = startDate.withMinute(0).withSecond(0).withNano(0); break;
            case DAYS: startDate = startDate.withHour(0).withMinute(0).withSecond(0).withNano(0); break;
            case MONTHS: startDate = startDate.withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0).withNano(0); break;
        }
        while(startDate.compareTo(maxDate) <= 0){
            LocalDateTime nextDate = startDate.plus(((ChronoUnit)arguments.get("Gtime").get()).getDuration());
            LocalDateTime finalStartDate = startDate;
            long recordsInPeriod = logEntries.stream()
                    .filter(logEntry -> logEntry.getDateTime().compareTo(nextDate) < 0
                            && logEntry.getDateTime().compareTo(finalStartDate) >= 0)
                    .count();
            periodRecordsCount.putIfAbsent(startDate, 0L);
            periodRecordsCount.compute(startDate, (key, value) -> value + recordsInPeriod);
            startDate = nextDate;
        }
    }

    private void groupByUsername(List<LogEntry> logEntries, T container) {
        logEntries.forEach(logEntry -> {
            HashMap<String, Long> usernameRecordsCount = (HashMap) container;
            usernameRecordsCount.putIfAbsent(logEntry.getUsername(), 0L);
            usernameRecordsCount.compute(logEntry.getUsername(), (key, value) -> value + 1);
        });
//        Map<String, Long> abc = logEntries.stream()
//                .collect(Collectors.groupingBy(LogEntry::getUsername, Collectors.counting()));
    }

    private List<LogEntry> loadRecords(List<Path> files) {
        List<LogEntry> result = new ArrayList<>();
        files.stream()
                .filter(p -> p.toString().endsWith(".log"))
                .forEach(p -> {
                    try {
                        List<String> lines = new LinkedList<>(Files.lines(p).collect(Collectors.toList()));
                        result.addAll(parseRecords(lines, p.toString()));
                    } catch (IOException ignored) {
                        System.err.println("Cannot read file " + p);
                    }
                });

        return result;
    }

    private void filterByUsername(List<LogEntry> logEntries, String username){
        logEntries.removeIf(logEntry -> logEntry.getUsername().compareTo(username) != 0);
    }

    private void filterByDateSince(List<LogEntry> logEntries, LocalDateTime since){
        logEntries.removeIf(logEntry -> !logEntry.getDateTime().isAfter(since));
    }

    private void filterByDateUntil(List<LogEntry> logEntries, LocalDateTime until){
        logEntries.removeIf(logEntry -> !logEntry.getDateTime().isBefore(until));
    }

    private void filterByMessage(List<LogEntry> logEntries, Pattern messagePattern){
        logEntries.removeIf(logEntry -> !messagePattern.matcher(logEntry.getMessage()).matches());
    }

    private List<LogEntry> parseRecords(List<String> records, String filename){
        List<LogEntry> result = new ArrayList<>();
        ListIterator<String> iterator = records.listIterator();
        while(iterator.hasNext()){
            Matcher matcher = logEntryPattern.matcher(iterator.next());
            if(matcher.matches()){
                try{
                    LocalDateTime dateTime = LocalDateTime.parse(matcher.group(1), dateTimeFormatter);
                    result.add(new LogEntry(dateTime, matcher.group(2), matcher.group(3)));
                } catch (DateTimeParseException ignored){
                    System.err.println("Cannot read line " + iterator.previousIndex() + " in file " + filename);
                }
            } else {
                System.err.println("Cannot read line " + iterator.previousIndex() + " in file " + filename);
            }
        }
        return result;
    }
}
