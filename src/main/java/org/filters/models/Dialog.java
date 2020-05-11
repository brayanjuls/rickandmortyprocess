package org.filters.models;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Splitter;


import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

@DefaultCoder(AvroCoder.class)
public class Dialog {
    private String[] fields;

    private enum Field {
        INDEX, LINE, EPISODE_NUMBER, SEASON_NUMBER, EPISODE_NAME, CHARACTER_NAME
    }

    public Dialog() {
        // for Avro
    }

    private String get(Field f) {
        return fields[f.ordinal()];
    }

    public static Dialog newRowDialog(String row) {
        List<String> pieces =new ArrayList<>();
        Splitter.on(Pattern.compile(",(?=(?:[^']*'[^']*')*[^']*$)")).split(row).iterator().forEachRemaining(r->pieces.add(r.replace("'","").trim()));
        Dialog dialog = new Dialog();
        String[] fields=new String[pieces.size()];
        dialog.fields = pieces.toArray(fields);
        return dialog;
    }


    public Integer getIndex() {
        return Integer.parseInt(get(Field.INDEX));
    }

    public String getCharacterName() {
        return get(Field.CHARACTER_NAME);
    }

    public String getEpisodeName() {
        return get(Field.EPISODE_NAME);
    }

    public String getLine() {
        return get(Field.LINE);
    }

    public Integer getEpisodeNumber() {
        return Integer.parseInt(get(Field.EPISODE_NUMBER));
    }

    public Integer getSeasonNumber() {
        return Integer.parseInt(get(Field.SEASON_NUMBER));
    }
}
