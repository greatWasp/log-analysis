package com;

import java.util.EnumSet;

public enum GroupingOptions {
    USERNAME,
    TIMEUNIT;

    public static final EnumSet<GroupingOptions> ALL = EnumSet.allOf(GroupingOptions.class);
}
