import com.iliasbolan.core.KeyValuePair;
import com.iliasbolan.core.Reducer;

import java.util.Iterator;

public class WordCountReducer implements Reducer {
    @Override
    public KeyValuePair reduce(String key, Iterator<String> values) {
        long sum = 0L;
        while (values.hasNext()) {
            sum += Long.parseLong(values.next().trim());
        }
        return new KeyValuePair(key, String.valueOf(sum));
    }
}
