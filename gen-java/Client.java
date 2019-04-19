import java.io.Console;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class Client {

    public static void main(String[] args) {
        try {
            // Get Configs
            Properties prop = new Properties();
            InputStream is = new FileInputStream("chordht.cfg");
            prop.load(is);
            Console console = System.console();
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
