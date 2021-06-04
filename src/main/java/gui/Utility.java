package gui;

import javax.swing.*;
import javax.swing.border.Border;
import javax.swing.border.CompoundBorder;
import javax.swing.border.EmptyBorder;
import javax.swing.border.MatteBorder;
import javax.swing.text.BadLocationException;
import java.awt.*;

public class Utility {

    static final String LEFT = "left";
    static final String BOTTOM = "bottom";
    static final String RIGHT = "right";

    // button strings and commands
    static final String CHOOSE_FILE = "Choose file";
    static final String ADD_FILE = "Add file to the hdfs";
    static final String MAPREDUCE = "Start mapreduce";
    static final String MAX = "max";
    static final String AVERAGE = "average";
    static final String STDEV = "stdev";
    static final String MEDIAN = "median";
    static final String SUM = "sum";
    static final String DELETE_FILE = "Delete file from hdfs";

    static CompoundBorder getOneLineBorder(String edge) {
        Border marginBorder = new EmptyBorder(20, 20, 20, 20);
        Border lineBorder;
        switch (edge) {
            case LEFT:
                lineBorder = new MatteBorder(0, 1, 0, 0, Color.BLACK);
                break;
            case BOTTOM:
                lineBorder = new MatteBorder(0, 0, 1, 0, Color.BLACK);
                break;
            case RIGHT:
                lineBorder = new MatteBorder(0, 0, 0, 1, Color.BLACK);
                break;
            default:
                lineBorder = new MatteBorder(1, 1, 1, 1, Color.BLACK);
        }

        return new CompoundBorder(marginBorder, lineBorder);
    }

    static void changeLastLine(JTextArea textArea, String newStr) {
        int lineCount = textArea.getLineCount();
        int lastLineStartOffset = 0;
        int lastLineEndOffset = 0;
        try {
            lastLineStartOffset = textArea.getLineStartOffset(lineCount - 1);
            lastLineEndOffset = textArea.getLineEndOffset(lineCount - 1);

        } catch (BadLocationException e) {
            e.printStackTrace();
        }
        textArea.replaceRange(newStr, lastLineStartOffset, lastLineEndOffset);
    }

}
