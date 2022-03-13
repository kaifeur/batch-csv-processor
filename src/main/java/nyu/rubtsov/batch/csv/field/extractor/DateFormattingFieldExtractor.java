package nyu.rubtsov.batch.csv.field.extractor;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.springframework.batch.item.file.transform.FieldExtractor;
import org.springframework.beans.factory.InitializingBean;

public class DateFormattingFieldExtractor<T> implements FieldExtractor<T>, InitializingBean {

    private final SimpleDateFormat format;
    private final FieldExtractor<T> delegate;

    public DateFormattingFieldExtractor(SimpleDateFormat format, FieldExtractor<T> delegate) {
        this.format = format;
        this.delegate = delegate;
    }

    @Override
    public Object[] extract(T item) {
        Object[] fields = delegate.extract(item);

        for (int i = 0; i < fields.length; i++) {
            if (fields[i] instanceof Date date) {
                fields[i] = format.format(date);
            }
        }

        return fields;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (delegate instanceof InitializingBean initializingBean) {
            initializingBean.afterPropertiesSet();
        }
    }
}
