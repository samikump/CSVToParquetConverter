package none.csvtoparquetconverter;

import java.util.Locale;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import org.apache.avro.*;
import org.apache.avro.generic.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.*;
import org.apache.parquet.hadoop.*;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.*;

import javax.swing.*;
import java.awt.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.List;

/**
 *
 * @author samikump
 * 
 * The program converts a CSV file to a Parquet file.
 * This is done as follows:
 * 1. In the main menu, the user is asked "Which CSV file do you want to convert?"
 * The user can either write in the path and the file name or browse his/her computer.
 * 2. After the user presses OK, the program validates the CSV file.
 * 2.1. If CSV file fails the validation, the program gives feedback to the user and returns to the main menu.
 * 2.2. If CSV file passes validation, the program converts it to Apache Parquet format.
 * 3. After conversion, the program does In-Memory Validation as follows:
 * 3.1. Check if the names of the columns in the converted file match with the names of the column in the CSV file.
 * 3.2. Check if the schema (data types) in the converted file match with the schema (data types) of the CSV file.
 * 3.3. Check if the number of the rows matches with the number of rows in the CSV file.
 * 3.4. If converted file fails the validation, user is given feedback and we return to the main menu.
 * 3.5. If converted file passes the validation, we proceed.
 * 4. Write the successfully converted Parquet file to the same folder as the original CSV file.
 * (If the file already exists, user is asked if he/she wants to rewrite it. If not, we skip section 5 and proceed to section 6.)
 * 5. User is told that the conversion was successful and also the sizes of the original CSV file and the Parquet file and the relative size of the Parquet file in percentages.
 * 6. User is asked if he/she wants to convert another file.
 * 7. If user says yes, we return to the main menu. If no, then we close the program.
 */

public class CSVToParquetConverter {

    public static void main(String[] args) {
        SwingUtilities.invokeLater(CSVToParquetConverter::showMainMenu);
    } // main method ends here

    // 1. Main menu
    private static void showMainMenu() {
        while (true) {
            JPanel panel = new JPanel(new BorderLayout(5, 5));
            JTextField pathField = new JTextField(30);
            JButton browseButton = new JButton("Browse");

            JPanel inputPanel = new JPanel(new BorderLayout(5, 5));
            inputPanel.add(new JLabel("Which CSV file do you want to convert?"), BorderLayout.NORTH);
            inputPanel.add(pathField, BorderLayout.CENTER);
            inputPanel.add(browseButton, BorderLayout.EAST);
            panel.add(inputPanel, BorderLayout.CENTER);

            browseButton.addActionListener(e -> {
                JFileChooser fileChooser = new JFileChooser();
                fileChooser.setFileFilter(new javax.swing.filechooser.FileNameExtensionFilter("CSV Files", "csv"));
                if (fileChooser.showOpenDialog(null) == JFileChooser.APPROVE_OPTION) {
                    pathField.setText(fileChooser.getSelectedFile().getAbsolutePath());
                }
            });

            int result = JOptionPane.showConfirmDialog(null, panel, "CSV to Parquet Converter", JOptionPane.OK_CANCEL_OPTION, JOptionPane.QUESTION_MESSAGE);

            if (result != JOptionPane.OK_OPTION) {
                System.exit(0);
            }

            File csvFile = new File(pathField.getText());
            if (!csvFile.exists() || !csvFile.isFile()) {
                JOptionPane.showMessageDialog(null, "Invalid file path. Please select a valid CSV file.", "Error", JOptionPane.ERROR_MESSAGE);
                continue;
            }

            try {
                processConversion(csvFile);
            } catch (Exception e) {
                JOptionPane.showMessageDialog(null, "An error occurred: " + e.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
                continue;
            }

            int again = JOptionPane.showConfirmDialog(null, "Do you want to convert another file?", "Continue?", JOptionPane.YES_NO_OPTION);
            if (again != JOptionPane.YES_OPTION) {
                System.exit(0);
            }
        } // while loop ends here
    } // showMainMenu method ends here

    private static void processConversion(File csvFile) throws Exception {
        // 2. Validate CSV
        List<String[]> csvData;
        try (CSVReader reader = new CSVReader(new FileReader(csvFile))) {
            csvData = reader.readAll();
        } catch (IOException | CsvException e) {
            throw new Exception("CSV Validation failed: " + e.getMessage());
        }

        if (csvData.isEmpty() || csvData.get(0).length == 0) {
            throw new Exception("CSV is empty or has no columns.");
        }

        String[] headers = csvData.get(0);
        List<String[]> rows = csvData.subList(1, csvData.size());

        // Create Avro Schema with Type Inference
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.record("CsvRecord").namespace("none.csvtoparquetconverter").fields();
        for (int i = 0; i < headers.length; i++) {
            String header = headers[i];
            // Sanitize header names for Avro
            String sanitizedHeader = header.replaceAll("[^a-zA-Z0-9_]", "_");
            if (sanitizedHeader.isEmpty() || Character.isDigit(sanitizedHeader.charAt(0))) {
                sanitizedHeader = "_" + sanitizedHeader;
            }
            
            Schema.Type inferredType = inferType(rows, i);
            if (inferredType == Schema.Type.INT) {
                fieldAssembler = fieldAssembler.name(sanitizedHeader).type().nullable().intType().noDefault();
            } else if (inferredType == Schema.Type.DOUBLE) {
                fieldAssembler = fieldAssembler.name(sanitizedHeader).type().nullable().doubleType().noDefault();
            } else {
                fieldAssembler = fieldAssembler.name(sanitizedHeader).type().nullable().stringType().noDefault();
            }
        }
        Schema schema = fieldAssembler.endRecord();

        // 3. In-Memory Conversion and Validation
        InMemoryOutputFile inMemoryOutputFile = new InMemoryOutputFile(); 
        
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(inMemoryOutputFile)
                .withSchema(schema)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {

            for (String[] row : rows) {
                GenericRecord record = new GenericData.Record(schema);
                for (int i = 0; i < headers.length; i++) {
                    String fieldName = schema.getFields().get(i).name();
                    String val = i < row.length ? row[i] : null;
                    
                    if (val == null || val.trim().isEmpty()) {
                        record.put(fieldName, null);
                    } else {
                        Schema fieldSchema = schema.getFields().get(i).schema();
                        Schema.Type type = getNonNullType(fieldSchema);
                        try {
                            if (type == Schema.Type.INT) {
                                record.put(fieldName, Integer.parseInt(val.trim()));
                            } else if (type == Schema.Type.DOUBLE) {
                                record.put(fieldName, Double.parseDouble(val.trim()));
                            } else {
                                record.put(fieldName, val);
                            }
                        } catch (NumberFormatException e) {
                            // Fallback to string if parsing fails (though inference should prevent this)
                            record.put(fieldName, val);
                        }
                    }
                }
                writer.write(record);
            }
        }

        // 3.1 - 3.3 In-Memory Validation
        validateParquet(inMemoryOutputFile.getInMemInputFile(), schema, rows.size());

        // 4. Write to disk
        File parquetFile = new File(csvFile.getParent(), csvFile.getName().replace(".csv", ".parquet"));
        
        if (parquetFile.exists()) {
            int response = JOptionPane.showConfirmDialog(null, 
                "File " + parquetFile.getName() + " already exists. Overwrite?", 
                "Confirm Overwrite", JOptionPane.YES_NO_OPTION);
            if (response != JOptionPane.YES_OPTION) {
                return;
            }
        }
        
        Files.write(parquetFile.toPath(), inMemoryOutputFile.toByteArray());

        // 5. Feedback
        double percentage = (parquetFile.length() / (double) csvFile.length()) * 100;
        String message = String.format(Locale.US, "Conversion successful!\n\nOriginal CSV size: %,d bytes\nParquet file size: %,d bytes\n" +
                "The size of the Parquet file is %.2f %% of the size of the CSV file!\n\nSaved to: %s",
                csvFile.length(), parquetFile.length(), percentage, parquetFile.getAbsolutePath());
        JOptionPane.showMessageDialog(null, message, "Success", JOptionPane.INFORMATION_MESSAGE);
    } // processConversion method ends here

    private static void validateParquet(InputFile inputFile, Schema expectedSchema, int expectedRows) throws Exception {
        int rowCount = 0;
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile)
                .withConf(new Configuration())
                .build()) {
            GenericRecord record;
            boolean first = true;
            while ((record = reader.read()) != null) {
                if (first) {
                    // 3.1 & 3.2 Check column names and types
                    List<Schema.Field> fields = record.getSchema().getFields();
                    List<Schema.Field> expectedFields = expectedSchema.getFields();
                    
                    if (fields.size() != expectedFields.size()) {
                        throw new Exception("Column count mismatch in converted file.");
                    }
                    
                    for (int i = 0; i < expectedFields.size(); i++) {
                        Schema.Field field = fields.get(i);
                        Schema.Field expectedField = expectedFields.get(i);
                        
                        if (!field.name().equals(expectedField.name())) {
                            throw new Exception(String.format("Column name mismatch at index %d. Expected: %s, Found: %s", i, expectedField.name(), field.name()));
                        }
                        
                        Schema.Type actualType = getNonNullType(field.schema());
                        Schema.Type expectedType = getNonNullType(expectedField.schema());
                        
                        if (actualType != expectedType) {
                            throw new Exception(String.format("Column type mismatch for %s. Expected: %s, Found: %s", field.name(), expectedType, actualType));
                        }
                    }
                    first = false;
                }
                rowCount++;
            }
        }

        // 3.3 Check row count
        if (rowCount != expectedRows) {
            throw new Exception(String.format("Row count mismatch. Expected: %d, Found: %d", expectedRows, rowCount));
        }
    } // validateParquet ends here

    private static Schema.Type inferType(List<String[]> rows, int colIndex) {
        boolean isInt = true;
        boolean isDouble = true;
        boolean hasData = false;
        for (String[] row : rows) {
            if (colIndex >= row.length || row[colIndex].trim().isEmpty()) continue;
            hasData = true;
            String val = row[colIndex].trim();
            if (isInt) {
                try {
                    Integer.parseInt(val);
                } catch (NumberFormatException e) {
                    isInt = false;
                }
            }
            if (isDouble) {
                try {
                    Double.parseDouble(val);
                } catch (NumberFormatException e) {
                    isDouble = false;
                }
            }
        }
        if (!hasData) return Schema.Type.STRING;
        if (isInt) return Schema.Type.INT;
        if (isDouble) return Schema.Type.DOUBLE;
        return Schema.Type.STRING;
    } // inferType method ends here

    private static Schema.Type getNonNullType(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            for (Schema s : schema.getTypes()) {
                if (s.getType() != Schema.Type.NULL) return s.getType();
            }
        }
        return schema.getType();
    } // getNonNullType method ends here

    // Helper classes for In-Memory Parquet operations
    private static class InMemoryOutputFile implements OutputFile {
        private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        @Override
        public PositionOutputStream create(long blockSizeHint) {
            return new DelegatingPositionOutputStream(baos) {
                @Override
                public long getPos() {
                    return baos.size();
                }
            };
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) {
            baos.reset();
            return create(blockSizeHint);
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 0;
        }

        public byte[] toByteArray() {
            return baos.toByteArray();
        }

        public InputFile getInMemInputFile() {
            return new InMemoryInputFile(baos.toByteArray());
        }
    } // class inMemoryOutputFile ends here

    private static class InMemoryInputFile implements InputFile {
        private final byte[] data;

        public InMemoryInputFile(byte[] data) {
            this.data = data;
        }

        @Override
        public long getLength() {
            return data.length;
        }

        @Override
        public SeekableInputStream newStream() {
            return new SeekableInputStream() {
                private int pos = 0;

                @Override
                public long getPos() {
                    return pos;
                }

                @Override
                public void seek(long newPos) {
                    pos = (int) newPos;
                }

                @Override
                public int read() {
                    return pos < data.length ? data[pos++] & 0xff : -1;
                }

                @Override
                public int read(byte[] b, int off, int len) {
                    if (pos >= data.length) return -1;
                    int toRead = Math.min(len, data.length - pos);
                    System.arraycopy(data, pos, b, off, toRead);
                    pos += toRead;
                    return toRead;
                }

                @Override
                public int read(ByteBuffer buf) throws IOException {
                    if (pos >= data.length) return -1;
                    int toRead = Math.min(buf.remaining(), data.length - pos);
                    buf.put(data, pos, toRead);
                    pos += toRead;
                    return toRead;
                }

                @Override
                public void readFully(byte[] b) throws IOException {
                    readFully(b, 0, b.length);
                }

                @Override
                public void readFully(byte[] b, int off, int len) throws IOException {
                    int read = read(b, off, len);
                    if (read < len) throw new EOFException();
                }

                @Override
                public void readFully(ByteBuffer buf) throws IOException {
                    int toRead = buf.remaining();
                    if (pos + toRead > data.length) throw new EOFException();
                    buf.put(data, pos, toRead);
                    pos += toRead;
                }
            };
        }
    } // class inMemoryInputFile ends here
} // class CSVToParquetConverter ends here