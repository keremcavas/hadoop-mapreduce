package hadoop;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

public class JobMonitor {

    private final Job job;
    private final HadoopController.JobListener jobListener;

    public JobMonitor(Job job, HadoopController.JobListener jobListener) {
        this.job = job;
        this.jobListener = jobListener;
    }

    public void monitor() {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {

                    if (job.isComplete()) {
                        timer.cancel();
                        timer.purge();
                    }

                    jobListener.refreshLastLine("map progress: " + (job.mapProgress() * 100) + "%  " +
                            "-  reeduce progress" + (job.reduceProgress() * 100) + "%");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }, 0, 3000);
    }
}
