package com;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;

public class CustomOptionGroup extends OptionGroup {

    private String selected;

    @Override
    public void setSelected(Option option) {
        if (option == null) {
            this.selected = null;
        } else {
            this.selected = ((CustomOption) option).getKey();
        }
    }
}
