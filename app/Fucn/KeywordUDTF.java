package com.ls.app.Fucn;

import com.ls.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {

    public void eval(String str) {
        //切分后放入list集合中
        List<String> list = KeywordUtil.splitKeyword(str);
        //遍历list集合
        for (String word : list) {
            //输出行级别，UDTF：输入一行，输出多行
            collect(Row.of(word));
        }
    }
}
