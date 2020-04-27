package cn.lgwen.kafka.tool.ip;

import java.io.*;

/**
 * 2020/3/26
 * aven.wu
 * danxieai258@163.com
 */
public class ReadData {

    public static void readFile(File file) throws IOException {
        InputStream inputStream = new FileInputStream(file);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "utf8"));
        String line;
        int index = 0;
        while ((line = reader.readLine()) != null && index < 10) {
            System.out.println(line);
            System.out.println();
            String strs[] = line.split("\t");
            index++;
        }
    }

    public static void main(String[] args) throws IOException {
        readFile(new File("G:\\工具包\\IP_ultimate_all_2020W10_multi_BD09_WGS84.txt"));
    }
}
