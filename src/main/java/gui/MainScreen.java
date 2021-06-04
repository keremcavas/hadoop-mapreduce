package gui;

import hadoop.HadoopController;
import utility.Time;

import javax.swing.*;
import java.awt.*;
import java.util.ArrayList;
import java.util.Arrays;

public class MainScreen {

    private final ButtonActionListener buttonActionListener;

    public static void main(String[] args) {
        MainScreen screen = new MainScreen();
        screen.show();
    }

    public MainScreen() {
        buttonActionListener = new ButtonActionListener();
    }

    private void setHeader(JFrame frame) {
        JPanel headerPanel = new JPanel();
        headerPanel.setLayout(new FlowLayout(FlowLayout.LEADING));
        headerPanel.setBorder(Utility.getOneLineBorder(Utility.BOTTOM));
        headerPanel.setPreferredSize(new Dimension(800, 80));

        JLabel hdfsLabel = new JLabel("Ready", SwingConstants.LEFT);
        hdfsLabel.setVerticalAlignment(SwingConstants.TOP);
        hdfsLabel.setPreferredSize(new Dimension(400, 20));

        ButtonActionListener.MessageListener messageListener = hdfsLabel::setText;
        buttonActionListener.attachMessageListener(messageListener);

        TextButton chooseFileButton = new TextButton();
        chooseFileButton.setText(Utility.CHOOSE_FILE);
        chooseFileButton.addActionListener(buttonActionListener);

        TextButton addFileButton = new TextButton();
        addFileButton.setText(Utility.ADD_FILE);
        addFileButton.addActionListener(buttonActionListener);
        // addFileButton.setMargin(new Insets(0, 0, 0, 0));

        TextButton deleteFileButton = new TextButton();
        deleteFileButton.setText(Utility.DELETE_FILE);
        deleteFileButton.addActionListener(buttonActionListener);

        headerPanel.add(hdfsLabel);
        headerPanel.add(addFileButton);
        headerPanel.add(chooseFileButton);
        headerPanel.add(deleteFileButton);

        frame.add(headerPanel, BorderLayout.NORTH);
    }

    private void setLeftMenu(JPanel mainPanel) {
        JPanel leftMenuPanel = new JPanel();
        // leftMenuPanel.setLayout(new BoxLayout(leftMenuPanel, BoxLayout.Y_AXIS));
        leftMenuPanel.setBorder(Utility.getOneLineBorder(Utility.RIGHT));
        leftMenuPanel.setPreferredSize(new Dimension(205, 360));
        leftMenuPanel.setMaximumSize(new Dimension(205, 360));

        TextButton mapreduceButton = new TextButton();
        mapreduceButton.setText(Utility.MAPREDUCE);

        TextButton maxButton = new TextButton(Utility.MAX);

        TextButton averageButton = new TextButton(Utility.AVERAGE);

        TextButton stDevButton = new TextButton(Utility.STDEV);

        TextButton medianButton = new TextButton(Utility.MEDIAN);

        TextButton sumButton = new TextButton(Utility.SUM);

        ArrayList<Component> items = new ArrayList<>(Arrays.asList(
                mapreduceButton, maxButton, averageButton, stDevButton, medianButton, sumButton));

        for (Component component : items) {
            leftMenuPanel.add(component);

            component.setPreferredSize(new Dimension(140, 30));
            component.setMaximumSize(new Dimension(140, 30));
            component.setMinimumSize(new Dimension(140, 30));

            if (component.getClass() == TextButton.class) {
                ((TextButton) component).addActionListener(buttonActionListener);
            }

            // ((JButton) component).setBorder(new EmptyBorder(30, 30, 30, 30));
        }

        mainPanel.add(leftMenuPanel, BorderLayout.WEST);
    }

    private void setMainPanel(JFrame frame) {
        JPanel mainPanel = new JPanel();
        mainPanel.setLayout(new BoxLayout(mainPanel, BoxLayout.X_AXIS));

        setLeftMenu(mainPanel);

        JPanel contentPanel = new JPanel();
        contentPanel.setLayout(new BoxLayout(contentPanel, BoxLayout.Y_AXIS));

        JTextArea textArea = new JTextArea();

        HadoopController.JobListener jobListener = new HadoopController.JobListener() {
            @Override
            public void addMessage(String s) {
                textArea.append(
                        System.lineSeparator() + Time.dateWithMilliseconds(System.currentTimeMillis()) + " => " + s);
            }

            @Override
            public void addMessage(long timestamp, String s) {
                textArea.append(System.lineSeparator() + Time.dateWithMilliseconds(timestamp) + " => " + s);
            }

            @Override
            public void refreshLastLine(String message) {
                Utility.changeLastLine(textArea,
                        Time.dateWithMilliseconds(System.currentTimeMillis()) + " => " + message);
            }

            @Override
            public void clear() {
                textArea.setText("");
            }
        };
        HadoopController.attachJobListener(jobListener);

        contentPanel.add(textArea);

        mainPanel.add(contentPanel);

        frame.add(mainPanel, BorderLayout.CENTER);
    }

    public void show() {
        JFrame frame = new JFrame();
        frame.setTitle("[BIG DATA] Mapreduce/home");
        frame.setSize(900, 500);
        frame.setLocation(100, 100);

        setHeader(frame);
        setMainPanel(frame);

        JLabel label = new JLabel("<html><div style='text-align: center;color: green;font-weight: 100;'>" +
                "keremCAVAS@ytuCE 2021</div></html>", SwingConstants.CENTER);
        frame.add(label, BorderLayout.SOUTH);

        frame.setVisible(true);
    }

}
