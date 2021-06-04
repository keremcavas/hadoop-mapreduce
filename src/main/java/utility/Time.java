package utility;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Time {

    public static String dateWithMilliseconds(long timestamp) {
        DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
        Date date = new Date(timestamp);
        return dateFormat.format(date);
    }

}
