import com.iliasbolan.core.Context;
import com.iliasbolan.core.Mapper;

public class WordCountMapper implements Mapper {
    @Override
    public void map(String value, Context context) {
        if (value == null || value.isBlank()) {
            return;
        }
        String[] tokens = value.trim().split("\\s+");
        for (String token : tokens) {
            if (!token.isEmpty()) {
                context.write(token, "1");
            }
        }
    }
}
