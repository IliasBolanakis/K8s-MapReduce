import com.iliasbolan.core.Combiner;
import com.iliasbolan.core.KeyValuePair;

import java.util.Iterator;

public class WordCountCombiner implements Combiner {
    @Override
    public KeyValuePair combine(String key, Iterator<String> values) {
        long sum = 0L;
        while (values.hasNext()) {
            sum += Long.parseLong(values.next().trim());
        }
        return new KeyValuePair(key, String.valueOf(sum));
    }
}
