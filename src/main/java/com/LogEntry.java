package com;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class LogEntry {
    private LocalDateTime dateTime;
    private String username;
    private String message;

    public LogEntry(LocalDateTime dateTime, String username, String message){
        this.dateTime = dateTime;
        this.username = username;
        this.message = message;
    }

    public LocalDateTime getDateTime(){
        return this.dateTime;
    }

    public String getUsername(){
        return this.username;
    }

    public String getMessage(){
        return this.message;
    }

    public void setDateTime(LocalDateTime dateTime) {
        this.dateTime = dateTime;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String format(DateTimeFormatter formatter){
        return dateTime.format(formatter) + " [" + username + "] : " + message;
    }

    @Override
    public String toString() {
        return dateTime.toString() + " [" + username + "] : " + message;
    }
}
