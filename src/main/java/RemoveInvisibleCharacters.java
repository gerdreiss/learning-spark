import com.google.common.io.Files;
import org.apache.commons.io.Charsets;

import java.io.File;
import java.io.IOException;

public class RemoveInvisibleCharacters {
    public static void main(String[] args) throws IOException {
        File file = new File("data/books.xml");
        String source = Files.toString(file, Charsets.UTF_8);
        String result = source.replaceAll("\\p{C}", "");
        Files.write(result, file, Charsets.UTF_8);
    }
}
