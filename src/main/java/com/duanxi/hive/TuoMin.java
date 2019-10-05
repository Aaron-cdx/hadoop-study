package com.duanxi.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @author caoduanxi
 * @2019/9/30 9:07
 */
public class TuoMin extends UDF {
    public Text evaluate(final Text s) {
        if (s == null) {
            return null;
        }
        String res = s.toString().substring(0, 3) + "*****";
        return new Text(res);
    }
}
