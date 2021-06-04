package gui;

import hadoop.HadoopController;

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

        HadoopController hadoopController = new HadoopController();

        switch (command) {

            case Utility.CHOOSE_FILE:
                JFileChooser fileChooserAdd = new JFileChooser(FileSystemView.getFileSystemView().getHomeDirectory());
                int result = fileChooserAdd.showOpenDialog(null);
                if (result == JFileChooser.APPROVE_OPTION) {
                    selectedFilePath = fileChooserAdd.getSelectedFile().getAbsolutePath();

                    System.out.println("[INFO] Selected file path: " + fileChooserAdd.getSelectedFile().getAbsolutePath());
                    System.out.println("[INFO] Selected file name: " + fileChooserAdd.getSelectedFile().getName());
                    pushMessage("HDFS => Selected file: " + fileChooserAdd.getSelectedFile().getName());
                } else {
                    selectedFilePath = null;
                    System.out.println("[INFO] FileChooser: cancelled");
                    pushMessage("HDFS => File selection cancelled");
                }
                break;

            case Utility.ADD_FILE:
                if (selectedFilePath != null) {
                    try {
                        result = hadoopController.addFile(selectedFilePath);
                        if (result == HadoopController.FILE_ADDED_SUCCESSFULLY) {
                            pushMessage("HDFS => File added successfully");
                        } else if (result == HadoopController.FILE_ALREADY_EXISTS) {
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

            case Utility.DELETE_FILE:
                JFileChooser fileChooserDelete = new JFileChooser(HadoopController.HDFS_FILE_DIRECTORY);
                int resultDelete = fileChooserDelete.showOpenDialog(null);
                if (resultDelete == JFileChooser.APPROVE_OPTION) {

                    String filePath = fileChooserDelete.getSelectedFile().getAbsolutePath();

                    System.out.println("[INFO] Selected file path: " + filePath);
                    System.out.println("[INFO] Selected file name: " + fileChooserDelete.getSelectedFile().getName());
                    pushMessage("HDFS => Deleting: " + fileChooserDelete.getSelectedFile().getName());

                    try {
                        if (hadoopController.deleteFile(filePath)) {
                            pushMessage("HDFS => File deleted successfully");
                        } else {
                            pushMessage("HDFS => File could not be deleted");
                        }
                    } catch (IOException ioException) {
                        pushMessage("HDFS => An error occurred while trying to delete file");
                        ioException.printStackTrace();
                    }
                } else {
                    pushMessage("HDFS => File selection cancelled");
                }
                break;

            case Utility.MAPREDUCE:
                try {
                    hadoopController.mapreduce();
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                    pushMessage("HDFS => An error occurred while trying mapreduce (IOException)");
                } catch (ClassNotFoundException classNotFoundException) {
                    classNotFoundException.printStackTrace();
                    pushMessage("HDFS => An error occurred while trying mapreduce (ClassNotFoundException)");
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
                    pushMessage("HDFS => An error occurred while trying mapreduce (InterruptedException)");
                }
                break;

            case Utility.MAX:
                try {
                    hadoopController.max();
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                    pushMessage("HDFS => An error occurred while trying mapreduce (IOException)");
                } catch (ClassNotFoundException classNotFoundException) {
                    classNotFoundException.printStackTrace();
                    pushMessage("HDFS => An error occurred while trying mapreduce (ClassNotFoundException)");
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
                    pushMessage("HDFS => An error occurred while trying mapreduce (InterruptedException)");
                }
                break;

            case Utility.AVERAGE:
                try {
                    hadoopController.average();
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                    pushMessage("HDFS => An error occurred while trying mapreduce (IOException)");
                } catch (ClassNotFoundException classNotFoundException) {
                    classNotFoundException.printStackTrace();
                    pushMessage("HDFS => An error occurred while trying mapreduce (ClassNotFoundException)");
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
                    pushMessage("HDFS => An error occurred while trying mapreduce (InterruptedException)");
                }
                break;

            case Utility.MEDIAN:
                try {
                    hadoopController.median();
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                    pushMessage("HDFS => An error occurred while trying mapreduce (IOException)");
                } catch (ClassNotFoundException classNotFoundException) {
                    classNotFoundException.printStackTrace();
                    pushMessage("HDFS => An error occurred while trying mapreduce (ClassNotFoundException)");
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
                    pushMessage("HDFS => An error occurred while trying mapreduce (InterruptedException)");
                }
                break;

            case Utility.STDEV:
                try {
                    hadoopController.standardDeviation();
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                    pushMessage("HDFS => An error occurred while trying mapreduce (IOException)");
                } catch (ClassNotFoundException classNotFoundException) {
                    classNotFoundException.printStackTrace();
                    pushMessage("HDFS => An error occurred while trying mapreduce (ClassNotFoundException)");
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
                    pushMessage("HDFS => An error occurred while trying mapreduce (InterruptedException)");
                }
                break;

            case Utility.SUM:
                try {
                    hadoopController.sum();
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                    pushMessage("HDFS => An error occurred while trying mapreduce (IOException)");
                } catch (ClassNotFoundException classNotFoundException) {
                    classNotFoundException.printStackTrace();
                    pushMessage("HDFS => An error occurred while trying mapreduce (ClassNotFoundException)");
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
                    pushMessage("HDFS => An error occurred while trying mapreduce (InterruptedException)");
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
        System.out.println("[MESSAGE] " + message);
    }
}
