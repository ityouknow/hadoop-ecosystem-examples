package com.neo;

public class HBaseTest {

    public static void main(String[] args) {
        System.err.println("start...");
        String tableName = "student";
        try{

            // 第一步：创建数据库表：“users”
            String[] columnFamilys = { "info", "course" };
            HBaseService.createTable(tableName, columnFamilys);

            // 第二步：向数据表的添加数据
            // 添加第一行数据
            HBaseService.putData(tableName, "tht", "info", "age", "20");
            HBaseService.putData(tableName, "tht", "info", "sex", "boy");
            HBaseService.putData(tableName, "tht", "course", "china", "97");
            HBaseService.putData(tableName, "tht", "course", "math", "128");
            HBaseService.putData(tableName, "tht", "course", "english", "85");
            // 添加第二行数据
            HBaseService.putData(tableName, "xiaoxue", "info", "age", "19");
            HBaseService.putData(tableName, "xiaoxue", "info", "sex", "boy");
            HBaseService.putData(tableName, "xiaoxue", "course", "china", "90");
            HBaseService.putData(tableName, "xiaoxue", "course", "math", "120");
            HBaseService.putData(tableName, "xiaoxue", "course", "english", "90");
            // 添加第三行数据
            HBaseService.putData(tableName, "qingqing", "info", "age", "18");
            HBaseService.putData(tableName, "qingqing", "info", "sex", "girl");
            HBaseService.putData(tableName, "qingqing", "course", "china", "100");
            HBaseService.putData(tableName, "qingqing", "course", "math", "100");
            HBaseService.putData(tableName, "qingqing", "course", "english", "99");
            // 第三步：获取一条数据
            System.out.println("获取一条数据");
            HBaseService.getRow(tableName, "tht");
            // 第四步：获取所有数据
            System.out.println("获取所有数据");
            HBaseService.scanAll(tableName);
            // 第五步：删除一条数据
            System.out.println("删除一条数据");
            HBaseService.deleteRow(tableName, "tht");
            HBaseService.scanAll(tableName);
            // 第六步：删除多条数据
            System.out.println("删除多条数据");
            String[] rows = { "xiaoxue", "qingqing" };
            HBaseService.delMultiRows(tableName, rows);
            HBaseService.scanAll(tableName);
            // 第八步：删除数据库
            //System.out.println("删除数据库");
            HBaseService.dropTable(tableName);

        } catch(Exception ex){
            ex.printStackTrace();
        }
        System.err.println("end...");
    }
}