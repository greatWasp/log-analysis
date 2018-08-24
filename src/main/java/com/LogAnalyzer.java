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

    private final Stream<Path> files;
    private final Optional<?>[] arguments;
    private final T container;

    public LogAnalyzer(Stream<Path> files, Optional<?>[] arguments, T container){
        this.files = files;
        this.arguments = arguments;
        this.container = container;
    }

    @Override
    public void run() {
        List<String> records = loadRecords(this.files);
        List<LogEntry> logEntries = parseRecords(records);

        handleFiltering(logEntries, this.arguments);
        handleGrouping(logEntries, this.arguments);
    }

    private void handleFiltering(List<LogEntry> logEntries, Optional<?>[] arguments){
        arguments[0].ifPresent(value -> filterByUsername(logEntries, (String)value));
        arguments[1].ifPresent(value -> filterByDateSince(logEntries, (LocalDateTime)value));
        arguments[2].ifPresent(value -> filterByDateUntil(logEntries, (LocalDateTime)value));
        arguments[3].ifPresent(value -> filterByMessage(logEntries, (Pattern)value));
    }

    private void handleGrouping(List<LogEntry> logEntries, Optional<?>[] arguments){
        if(logEntries.size() == 0){
            return;
        }
        if(arguments[5].isPresent()){
            LocalDateTime minDate = logEntries.stream().min(Comparator.comparing(LogEntry::getDateTime)).get().getDateTime();
            LocalDateTime maxDate = logEntries.stream().max(Comparator.comparing(LogEntry::getDateTime)).get().getDateTime();
            if(arguments[4].isPresent()){
                groupByUsernameAndDate(logEntries, this.container, minDate, maxDate);
            } else {
                groupByDate(logEntries, this.container, minDate, maxDate);
            }
        } else if(arguments[4].isPresent()){
            groupByUsername(logEntries, this.container);
        }
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
        switch((ChronoUnit)arguments[5].get()){
            case HOURS: startDate = startDate.withMinute(0).withSecond(0).withNano(0); break;
            case DAYS: startDate = startDate.withHour(0).withMinute(0).withSecond(0).withNano(0); break;
            case MONTHS: startDate = startDate.withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0).withNano(0); break;
        }
        while(startDate.compareTo(maxDate) <= 0){
            LocalDateTime nextDate = startDate.plus(((ChronoUnit)arguments[5].get()).getDuration());
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

    private List<String> loadRecords(Stream<Path> files) {
        List<String> result = new ArrayList<>();
        files
                .filter(p -> p.toString().endsWith(".log"))
                .forEach(p -> {
                    try {
                        result.addAll(Files.lines(p).collect(Collectors.toList()));
                    } catch (IOException ignored) {
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

    private List<LogEntry> parseRecords(List<String> records){
        List<LogEntry> result = new ArrayList<>();
        records.forEach(record -> {
            Matcher matcher = logEntryPattern.matcher(record);
            if(matcher.matches()){
                try{
                    LocalDateTime dateTime = LocalDateTime.parse(matcher.group(1), dateTimeFormatter);
                    result.add(new LogEntry(dateTime, matcher.group(2), matcher.group(3)));
                } catch (DateTimeParseException ignored){
                }
            }
        });
        return result;
    }
}
