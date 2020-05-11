package org.filters.dialogs;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.avro.Schema;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.filters.models.Dialog;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class FilterDialogs {

    public interface FilterDialogsOptions
            extends DataflowPipelineOptions {


        @Description("Path of the file to read from")
        @Default.String("dialogs")
        @Validation.Required
        String getTopicName();

        void setTopicName(String value);


        @Description("Path of the table to write to")
        @Default.String("rick.dialogs")
        @Validation.Required
        String getRickTableName();

        void setRickTableName(String value);

        @Description("Path of the table to write to")
        @Default.String("morty.dialogs")
        @Validation.Required
        String getMortyTableName();

        void setMortyTableName(String value);

        @Description("Symbolic word in dialogs")
        @Default.String("geez")
        @Validation.Required
        String getSymbolicWord();

        void setSymbolicWord(String value);


    }

    public static void main(String[] args) {
        FilterDialogs.FilterDialogsOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(FilterDialogs.FilterDialogsOptions.class);
        runFilterSymbolicWord(options);
    }

    private static void runFilterSymbolicWord(FilterDialogsOptions options) {
        Pipeline symbolicWordPipeline = Pipeline.create(options);
        String rickTable = String.format("%s:%s",options.getProject(),options.getRickTableName());
        String mortyTable = String.format("%s:%s",options.getProject() ,options.getMortyTableName());
        String topic = String.format("projects/%s/topics/%s",options.getProject(),options.getTopicName());


        String symbolicWord= options.getSymbolicWord();
        TableSchema outputTableSchema = getOutPutTableSchema();
        PCollection<KV<String, Dialog>> dialogsWithSymbolicWord = symbolicWordPipeline
                .apply("GetEvents", PubsubIO.readStrings().fromTopic(topic))
                .apply("ExtractDialog", ParDo.of(new DoFn<String, Dialog>() {
            @ProcessElement
            public void processElement(ProcessContext c)
            throws Exception {
                String row = c.element();
                Dialog dialog=Dialog.newRowDialog(row);
                c.output(dialog);
            }
        })).apply("findSymbolicWord", ParDo.of(new DoFn<Dialog, KV<String, Dialog>>() {
            @ProcessElement
            public void processElement(ProcessContext c)
            throws Exception {
                Dialog currentDialog = c.element();
                if (currentDialog.getLine().toLowerCase().contains(symbolicWord)) {
                        c.output(KV.of(currentDialog.getCharacterName(), currentDialog));
                }
            }
        }));

        dialogsWithSymbolicWord
                .apply("getRickDialogs",ParDo.of(new DoFn<KV<String,Dialog>,Dialog>(){
                        @ProcessElement
                        public void processElement(ProcessContext c) throws Exception{
                            String characterName = c.element().getKey();
                            if(!Objects.isNull(characterName) && characterName.equalsIgnoreCase("rick")){
                                c.output(c.element().getValue());
                            }
                        }
                }))
                .apply("rickDialogsToBQROW", ParDo.of(new DoFn<Dialog, TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws Exception {
                        TableRow row = new TableRow();
                        Dialog ricksDialog = c.element();
                        row.set("index", ricksDialog.getIndex());
                        row.set("character_name", ricksDialog.getCharacterName());
                        row.set("episode_name", ricksDialog.getEpisodeName());
                        row.set("episode_number", ricksDialog.getEpisodeNumber());
                        row.set("season_number", ricksDialog.getSeasonNumber());
                        row.set("line", ricksDialog.getLine());
                        c.output(row);
                     }
                }))
                .apply(BigQueryIO.writeTableRows().to(rickTable)
                        .withSchema(outputTableSchema)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        dialogsWithSymbolicWord
                .apply("getMortyDialogs",ParDo.of(new DoFn<KV<String,Dialog>,Dialog>(){
                    @ProcessElement
                    public void processElement(ProcessContext c) throws Exception{
                        String characterName = c.element().getKey();
                        if(!Objects.isNull(characterName) && characterName.equalsIgnoreCase("morty")){
                            c.output(c.element().getValue());
                        }
                    }
                }))
                .apply("mortyDialogsToBQROW", ParDo.of(new DoFn<Dialog, TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws Exception {
                        TableRow row = new TableRow();
                        Dialog mortyDialog = c.element();
                        row.set("index", mortyDialog.getIndex());
                        row.set("character_name", mortyDialog.getCharacterName());
                        row.set("episode_name", mortyDialog.getEpisodeName());
                        row.set("episode_number", mortyDialog.getEpisodeNumber());
                        row.set("season_number", mortyDialog.getSeasonNumber());
                        row.set("line", mortyDialog.getLine());
                        c.output(row);
                    }
                }))
                .apply(BigQueryIO.writeTableRows().to(mortyTable)
                        .withSchema(outputTableSchema)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        symbolicWordPipeline.run();
    }

    private static TableSchema getOutPutTableSchema(){
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("index").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("character_name").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("episode_name").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("episode_number").setType("INTEGER").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("season_number").setType("INTEGER").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("line").setType("STRING").setMode("REQUIRED"));
        return new TableSchema().setFields(fields);
    }
}
