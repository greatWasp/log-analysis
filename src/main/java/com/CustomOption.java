package com;

import org.apache.commons.cli.Option;

public class CustomOption extends Option {
    private final String opt;
    private String longOpt;

    public CustomOption(String opt, boolean hasArg, String description) throws IllegalArgumentException {
        super(opt, (String)null, hasArg, description);
        this.opt = opt;
    }

    String getKey() {
        return this.opt == null ? this.longOpt : this.opt;
    }
}
