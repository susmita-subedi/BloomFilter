import java.util.HashMap;
import java.util.Map;

public class MRDPUtil {
public static Map<String, String> transformXmlToMap(String xml) {
 Map<String, String> map = new HashMap<String, String>();
 try {
 String[] tokens = xml.trim().substring(6, xml.trim().length() - 2).split("\"");
 for (int i = 0; i < tokens.length - 1; i += 2) {
 String key = tokens[i].trim();
 String val = tokens[i + 1];
 map.put(key.substring(0, key.length() - 1), val);
 }
 } catch (StringIndexOutOfBoundsException e) {
 System.err.println(xml);
 }
 return map;
 }
}