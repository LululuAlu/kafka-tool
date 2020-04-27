package cn.lgwen.kafka.tool.ip;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author: wuxuyang
 * @Date: 2019-03-07
 */
public class FileUtil {

    public static String readClasspathFile(String filePath) throws IOException {
        InputStream inputStream = FileUtil.class.getClassLoader().getResourceAsStream(filePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "utf8"));
        StringBuilder bf = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            bf.append(line);
        }
        return bf.toString();
    }

    /**
     * 获取目录下的所有文件名称 没有递归子目录
     * @param dirPath  文件目录
     * @return 文件名称集合
     */
    public static List<String> readDirFile(String dirPath) {
        URL url = FileUtil.class.getClassLoader().getResource(dirPath);
        if (null == url) {
            return null;
        }
        String[] list = new File(url.getPath()).list();
        if (null == list) {
            return null;
        }
        return Arrays.asList(list);
    }

    public static String readFile(String filePath) throws IOException {
        return readFile(new File(filePath));
    }

    public static String readFile(File file) throws IOException {
        InputStream inputStream = new FileInputStream(file);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "utf8"));
        StringBuilder bf = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            bf.append(line);
        }
        return bf.toString();
    }


    public static List<String> readFileAsList(String filePath) throws IOException {
        InputStream inputStream = new FileInputStream(filePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "utf8"));
        List<String> list = new ArrayList<>();
        String line;
        while ((line = reader.readLine()) != null) {
            list.add(line);
        }
        return list;
    }


    public static List<String> readClassPathFileAsList(String filePath) throws IOException {
        InputStream inputStream = FileUtil.class.getClassLoader().getResourceAsStream(filePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "utf8"));
        List<String> list = new ArrayList<>();
        String line;
        while ((line = reader.readLine()) != null) {
            list.add(line);
        }
        return list;
    }


    public static void writeToFile(String data, String path) throws IOException {
        File file = new File(path);
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        writer.write(data);
        writer.flush();
    }
}
