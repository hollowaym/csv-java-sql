
/**
 * App
 */
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.io.*;
import java.sql.*;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.AbstractRowProcessor;
import com.univocity.parsers.common.processor.RowProcessor;
import com.univocity.parsers.csv.*;

public class App {

    public static PreparedStatement pstmt;
    private static int rowsRecieved;
    private static int rowsSuccessful;
    private static int rowsFailed;

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Error, usage: java App inputfile");
            System.exit(1);
        }
        // check for input file
        File inputFile = new File(args[0]);
        if (inputFile.exists() & inputFile.isFile() & inputFile.canRead()) {
            Path theCSVPath = inputFile.toPath();
            String contentType = "";
            try {
                contentType = Files.probeContentType(theCSVPath);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            // Strict encodings, can add support for more, or remove on request.
            if ("text/csv".equals(contentType) || "application/vnd.ms-excel".equals(contentType)) {

            } else {
                System.err.println("Error, input must be .csv");
                System.exit(1);
            }
        } else {
            System.err.println("Error, file is missing or corrupt");
            System.exit(1);
        }

        // Security risk, directory traversal
        // Create files for output
        String newFileName = inputFile.getName();
        int pos = newFileName.lastIndexOf(".");
        if (pos > 0) {
            newFileName = newFileName.substring(0, pos);
        }
        File outFile = new File(inputFile.getParentFile() + "\\" + newFileName + "-bad.csv");
        File logFile = new File(inputFile.getParentFile() + "\\" + newFileName + ".log");

        // check for and delete old DB.
        deleteDB(newFileName);

        Connection sqliteConnection = null;
        Statement stmt = null;
        try {
            Class.forName("org.sqlite.JDBC");
            Class.forName("com.univocity.parsers.csv.CsvParser");
            sqliteConnection = DriverManager.getConnection("jdbc:sqlite:" + newFileName + ".db");
            stmt = sqliteConnection.createStatement(); // TODO better error handleing
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        // I have no idea what these could represent without asking the client. Best to
        // save it all as Strings for now.
        String sqlCreateTable = "CREATE TABLE CLIENTS " + "(A VARCHAR(255) not NULL, " + "B VARCHAR(255) not NULL, "
                + "C VARCHAR(255) not NULL, " + "D VARCHAR(255) not NULL, " + "E VARCHAR(255) not NULL, "
                + "F VARCHAR(255) not NULL, " + "G VARCHAR(255) not NULL, " + "H VARCHAR(255) not NULL, "
                + "I VARCHAR(255) not NULL, " + "J VARCHAR(255) not NULL," + " PRIMARY KEY (A,B,C,D,E,F,G,H,I,J))";

        Savepoint theSavepoint = null;
        try {
            stmt.executeUpdate(sqlCreateTable);
            final String sqlInsert = "INSERT INTO CLIENTS(A,B,C,D,E,F,G,H,I,J) VALUES (?,?,?,?,?,?,?,?,?,?)";
            pstmt = sqliteConnection.prepareStatement(sqlInsert);
            sqliteConnection.setAutoCommit(false);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            doPanic(newFileName, logFile, outFile, inputFile);
            e.printStackTrace();
        }

        readAndWrite(inputFile, outFile, "UTF-8"); // TODO Only checking for null and over row limit, add more checks?
                                                   // Also, check header.
        try {
            // Should do batch inserts, but error handling
            sqliteConnection.commit();
            pstmt.close();
            stmt.close();
            sqliteConnection.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            doPanic(newFileName, logFile, outFile, inputFile);
            e.printStackTrace();
        }

        try {
            FileWriter myWriter = new FileWriter(logFile);
            myWriter.write("Records received: " + rowsRecieved + "\nRecords successful: " + rowsSuccessful
                    + "\nRecords failed: " + rowsFailed);
            myWriter.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            doPanic(newFileName, logFile, outFile, inputFile);
            e.printStackTrace();
        }
        // TODO validate that csvRowCount = bad.csv rows + Database rows
    }

    private static int InsertRow(PreparedStatement theStatement, String[] theStrings) {
        try {
            theStatement.setString(1, theStrings[0]);
            theStatement.setString(2, theStrings[1]);
            theStatement.setString(3, theStrings[2]);
            theStatement.setString(4, theStrings[3]);
            theStatement.setString(5, theStrings[4]);
            theStatement.setString(6, theStrings[5]);
            theStatement.setString(7, theStrings[6]);
            theStatement.setString(8, theStrings[7]);
            theStatement.setString(9, theStrings[8]);
            theStatement.setString(10, theStrings[9]);
            theStatement.executeUpdate();
            return 0;
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            return e.getErrorCode(); // A single needle. This error does not propogate corretly.
        }
    }

    private static CsvWriter createCsvWriter(File output, String encoding) {
        CsvWriterSettings settings = new CsvWriterSettings();
        // configure the writer ...
        settings.setHeaders("A", "B", "C", "D", "E", "F", "G", "H", "I", "J");
        settings.getFormat().setLineSeparator("\n");
        try {
            return new CsvWriter(new OutputStreamWriter(new FileOutputStream(output), encoding), settings);
        } catch (IOException e) {
            throw new IllegalArgumentException("Error writing to " + output.getAbsolutePath(), e);
        }
    }

    private static RowProcessor createRowProcessor(File output, String encoding) {
        final CsvWriter writer = createCsvWriter(output, encoding);
        return new AbstractRowProcessor() {
            // TODO better flow control and state
            @Override
            public void rowProcessed(String[] row, ParsingContext context) {
                rowsRecieved++;
                if (shouldWriteRow(row)) {
                    writer.writeRow(row);
                    rowsFailed++;
                } else {
                    // Try insert into DB, if error, probably duplicate
                    rowsSuccessful++;
                    if (InsertRow(pstmt, row) != 0) {
                        rowsSuccessful--;
                        rowsFailed++;
                        writer.writeRow(row);
                    }
                }
            }

            // TODO format better, bad return structure
            private boolean shouldWriteRow(String[] row) {
                if (row.length > 10) {
                    return true;
                }
                for (String string : row) {
                    if (string == null) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public void processEnded(ParsingContext context) {
                writer.close();
            }
        };
    }

    private static void readAndWrite(File input, File output, String encoding) {
        CsvParserSettings settings = new CsvParserSettings();
        // configure the parser here
        settings.setHeaderExtractionEnabled(false);
        settings.getFormat().setLineSeparator("\n");
        settings.setHeaders("A", "B", "C", "D", "E", "F", "G", "H", "I", "J");
        // tells the parser to send each row to them custom processor, which will
        // validate and redirect all rows to the CsvWriter
        settings.setProcessor(createRowProcessor(output, encoding));

        CsvParser parser = new CsvParser(settings);
        try {
            parser.parse(new InputStreamReader(new FileInputStream(input), encoding));
        } catch (IOException e) {
            throw new IllegalStateException("Unable to open input file " + input.getAbsolutePath(), e);
        }
    }

    private static void deleteDB(String newFileName) {
        Path currentRelativePath = Paths.get("");
        String s = currentRelativePath.toAbsolutePath().toString();
        File dbFile = new File(s + "\\" + newFileName + ".db");
        if ((dbFile).exists() & dbFile.isFile() & dbFile.canRead()) {
            dbFile.delete();
        }
    }

    private static void doPanic(String newFileName, File logFile, File outFile, File inputFile) {
        deleteDB(newFileName); 
        try {
            FileWriter myWriter = new FileWriter(logFile);
            myWriter.write("Records received: unknown\nRecords successful: none\nRecords failed: all");
            myWriter.close();
            Files.copy(inputFile.toPath(), outFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.exit(0);
    }
}
