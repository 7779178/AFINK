package com.ls.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeywordUtil {

    public static List<String> splitKeyword(String value) {

        //创建集合用于存放切分以后的单词
        ArrayList<String> result = new ArrayList<>();
        //创建字符串读取器
        StringReader reader = new StringReader(value);
        //使用最粗粒度切分
        IKSegmenter ikSegmenter = new IKSegmenter(reader, false);

        //切词
        try {
            Lexeme next = ikSegmenter.next();

            while (next != null) {

                //取出单词
                String word = next.getLexemeText();
                result.add(word);
                next = ikSegmenter.next();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        //返回数据
        return result;
    }

    public static void main(String[] args) {

        System.out.println(splitKeyword("我爱我的祖国"));

    }
}
