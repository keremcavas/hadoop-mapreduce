package gui;

import hadoop.HdfsController;

import javax.swing.*;
import javax.swing.filechooser.FileSystemView;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.util.ArrayList;

public class ButtonActionListener implements ActionListener {

    public ButtonActionListener() {
        if (messageListeners == null) {
            messageListeners = new ArrayList<>();
        }
    }

    public interface MessageListener {
        void push(String message);
    }

    private String selectedFilePath = null;

    private static ArrayList<MessageListener> messageListeners;

    @Override
    public void actionPerformed(ActionEvent e) {
        String command = e.getActionCommand();

        switch (command) {

            case Utility.CHOOSE_FILE:
                JFileChooser fileChooser = new JFileChooser(FileSystemView.getFileSystemView().getHomeDirectory());
                int result = fileChooser.showOpenDialog(null);
                if (result == JFileChooser.APPROVE_OPTION) {
                    selectedFilePath = fileChooser.getSelectedFile().getAbsolutePath();

                    System.out.println("[INFO] Selected file path: " + fileChooser.getSelectedFile().getAbsolutePath());
                    System.out.println("[INFO] Selected file name: " + fileChooser.getSelectedFile().getName());
                    pushMessage("HDFS => Selected file: " + fileChooser.getSelectedFile().getName());
                } else {
                    selectedFilePath = null;
                    System.out.println("[INFO] FileChooser: cancelled");
                    pushMessage("HDFS => File selection cancelled");
                }
                break;

            case Utility.ADD_FILE:
                if (selectedFilePath != null) {
                    HdfsController hdfsController = new HdfsController();
                    try {
                        result = hdfsController.addFile(selectedFilePath);
                        if (result == HdfsController.FILE_ADDED_SUCCESSFULLY) {
                            pushMessage("HDFS => File added successfully");
                        } else if (result == HdfsController.FILE_ALREADY_EXISTS) {
                            pushMessage("HDFS => File already exists");
                        }

                    } catch (IOException ioException) {
                        ioException.printStackTrace();
                        pushMessage("HDFS => An error occurred while trying to add a file");
                    }
                } else {
                    System.out.println("[ERROR] File have not chosen yet");
                    pushMessage("HDFS => File have not chosen yet");
                }
                break;

            default:
                System.out.println("[ERROR] wrong button selection (command = '" + command + "')");
        }
    }

    public void attachMessageListener(MessageListener listener) {
        messageListeners.add(listener);
    }

    private void pushMessage(String message) {
        for (MessageListener messageListener : messageListeners) {
            messageListener.push(message);
        }
    }
}
