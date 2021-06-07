package hadoop;

public class JobTrackerResult {

    public static final int APPEND = 900;
    public static final int REFRESH = 901;
    public static final int CLEAR = 902;

    private final int type;
    private final long timestamp;
    private final String message;

    private static HadoopController.JobListener jobListener;

    public JobTrackerResult(int type, String message) {
        this.type = type;
        this.message = message;
        timestamp = -1;
    }

    public static void setJobListener(HadoopController.JobListener jobListener) {
        JobTrackerResult.jobListener = jobListener;
    }


    public void publish() {
        switch (type) {
            case APPEND:
                if (timestamp == -1) {
                    jobListener.addMessage(message);
                } else {
                    jobListener.addMessage(timestamp, message);
                }
                break;

            case REFRESH:
                jobListener.refreshLastLine(message);
                break;

            case CLEAR:
                jobListener.clear();
                break;
        }
    }
}
