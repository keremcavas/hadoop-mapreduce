package gui;

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

        headerPanel.add(hdfsLabel);
        headerPanel.add(addFileButton);
        headerPanel.add(chooseFileButton);

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

        frame.add(mainPanel, BorderLayout.SOUTH);
    }

    public void show() {
        JFrame frame = new JFrame();
        frame.setTitle("[BIG DATA] Mapreduce/home");
        frame.setSize(800, 500);
        frame.setLocation(100, 100);

        setHeader(frame);
        setMainPanel(frame);

        frame.setVisible(true);
    }

}
