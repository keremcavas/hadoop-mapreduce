package hadoop;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

public class JobMonitor {

    private final Job job;
    private final HadoopController.JobListener jobListener;

    private static UITrigger uiTrigger;

    public interface UITrigger {
        void push(JobTrackerResult jobTrackerResult);
    }

    public JobMonitor(Job job, HadoopController.JobListener jobListener) {
        this.job = job;
        this.jobListener = jobListener;
    }

    public static void setUiTrigger(UITrigger uiTrigger) {
        JobMonitor.uiTrigger = uiTrigger;
    }

    public void monitor() {

        JobTrackerResult.setJobListener(jobListener);
        int currentJobCount = 1;

        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {

                    if (job.getJobState() == JobStatus.State.RUNNING) {

                        if (job.isComplete()) {
                            timer.cancel();
                            timer.purge();

                            uiTrigger.push(new JobTrackerResult(JobTrackerResult.REFRESH, "map progress: 100%  " +
                                    "-  reeduce progress: 100%"));
                        }

                        long totalJobCount = job.getCounters().getGroup("Map-Reduce Framework")
                                .findCounter("Map input records").getValue();

                        uiTrigger.push(new JobTrackerResult(JobTrackerResult.REFRESH,
                                "map progress: " + (job.mapProgress() * 100) + "%  " + "-  reeduce progress" +
                                        (job.reduceProgress() * 100) + "% (" + currentJobCount + "/" +
                                        totalJobCount + ")"));

                    } else if (job.getJobState() == JobStatus.State.SUCCEEDED) {
                        timer.cancel();
                        timer.purge();

                        uiTrigger.push(new JobTrackerResult(JobTrackerResult.REFRESH, "map progress: 100%  " +
                                "-  reeduce progress: 100%"));
                    } else if (job.getJobState() == JobStatus.State.FAILED ||
                            job.getJobState() == JobStatus.State.KILLED) {
                        timer.cancel();
                        timer.purge();
                    }
                } catch (IOException | InterruptedException | IllegalStateException e) {
                    e.printStackTrace();
                }
            }
            }, 0, 1000);
    }
}
