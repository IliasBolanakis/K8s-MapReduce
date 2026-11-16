import com.iliasbolan.core.Context;
import com.iliasbolan.core.Mapper;

public class CsvSumMapper implements Mapper {
    @Override
    public void map(String value, Context context) {
        if (value == null || value.isBlank()) {
            return;
        }
        String[] parts = value.trim().split(",");
        if (parts.length >= 2) {
            String key = parts[0].trim();
            String amount = parts[1].trim();
            if (!"category".equalsIgnoreCase(key) && !key.isEmpty()) {
                context.write(key, amount);
            }
        }
    }
}
