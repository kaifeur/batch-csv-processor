package nyu.rubtsov.batch.csv.conversion;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.TypeDescriptor;

public class DateConversionService implements ConversionService {

    private final List<SimpleDateFormat> formats;

    public DateConversionService(List<SimpleDateFormat> formats) {
        this.formats = formats;
    }

    @Override
    public boolean canConvert(Class<?> sourceType, Class<?> targetType) {
        return sourceType == String.class && targetType == Date.class;
    }

    @Override
    public boolean canConvert(TypeDescriptor sourceType, TypeDescriptor targetType) {
        return canConvert(sourceType.getType(), targetType.getType());
    }

    @Override
    public <T> T convert(Object source, Class<T> targetType) {
        if (!(source instanceof String dateAsStr)) {
            throw new IllegalArgumentException("Unsupported source: " + source);
        }

        if (targetType != Date.class) {
            throw new IllegalArgumentException("Unsupported target type: " + targetType);
        }

        String cleanedDateAsStr = dateAsStr.replaceAll("(?<=\\d)(st|nd|rd|th)", "");

        Date date = null;
        ParseException parseException = null;

        for (SimpleDateFormat format : formats) {
            try {
                date = format.parse(cleanedDateAsStr);
            } catch (ParseException e) {
                if (parseException == null) {
                    parseException = e;
                } else {
                    parseException.addSuppressed(e);
                }
            }
        }

        if (date == null && parseException != null) {
            throw new RuntimeException(parseException);
        }

        return (T) date;
    }

    @Override
    public Object convert(Object source, TypeDescriptor sourceType, TypeDescriptor targetType) {
        return convert(source, targetType.getType());
    }
}
