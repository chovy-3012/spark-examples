package spark.hbase.test;

import org.apache.commons.lang.math.RandomUtils;

import java.util.UUID;

public class BuildData {

    public static void main(String[] args) {
    }


    private static String[] district1 = new String[]{"经济技术开发区", "综合保税区", "花桥经济开发区", "旅游度假区", "玉山镇", "巴城镇", "花桥镇",
            "周市镇", "千灯镇", "陆家镇", "张浦镇", "周庄镇", "锦溪镇", "淀山湖镇"};
    private static String[] district2 = new String[]{"人民路", "阳光东路", "长江南路", "长江北路", "花苑路", "新吴街", "增进路", "海虹路", "商鞅路",
            "宝觉街", "京东路", "港浦中路", "桃源路", "俱进路", "三家路", "园圃路", "前进东路", "长江中路", "珠江北路", "南后街"};

    // 获取区域
    private static String getDistrict() {
        StringBuilder d = new StringBuilder();
        int nextInt = RandomUtils.nextInt(district1.length - 1);
        d.append(district1[nextInt]);
        int nextInt2 = RandomUtils.nextInt(district2.length - 1);
        d.append(district2[nextInt2]);
        int nextInt3 = RandomUtils.nextInt(30);
        d.append("点位").append((nextInt3 + 1));
        return d.toString();
    }

    //获取区域1
    private static String getDistrict1() {
        int nextInt = RandomUtils.nextInt(district1.length - 1);
        return district1[nextInt];
    }

    //获取区域2
    private static String getDistrict2() {
        int nextInt = RandomUtils.nextInt(district2.length - 1);
        return district2[nextInt];
    }

    // 获取id
    private static String getId() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    // 获取mac
    private static String getMac() {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < 6; i++) {
            int nextInt = RandomUtils.nextInt(255);
            stringBuilder.append(Integer.toHexString(nextInt));
            if (i != 5) {
                stringBuilder.append('-');
            }
        }
        return stringBuilder.toString();
    }

    // 获取时间
    private static String getTime() {
        return System.currentTimeMillis() + "";
    }

    // 获取rowkey
    private static String getRowkey(String mac, String district, String time) {
        return time + "|" + mac + "|" + district;
    }

}
