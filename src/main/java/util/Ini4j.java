package util;

import org.ini4j.Wini;
import java.io.File;
import java.io.IOException;

public class Ini4j {
    public static String filePath;

    public Ini4j(String filePath){
        Ini4j.filePath = filePath;
    }

    public String  readIni(String iniSection,String key) throws IOException {
        Wini ini = new Wini(new File(filePath));
        return ini.get(iniSection,key);
    }
}