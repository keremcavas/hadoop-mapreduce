package hadoop;

import javax.swing.*;
import java.util.List;

public class JobWorker extends SwingWorker<Void, JobTrackerResult> {

    @Override
    protected Void doInBackground() {
        return null;
    }

    @Override
    protected void process(List<JobTrackerResult> chunks) {
        for (JobTrackerResult chunk : chunks) {
            chunk.publish();
        }
    }
}
