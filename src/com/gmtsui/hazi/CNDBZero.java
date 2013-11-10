package com.gmtsui.hazi;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

public class CNDBZero {
    static class Table {
        final public String templateName;
        final public String localesName;
        final public String templateIds[];
        final public String localesIds[];
        final public String cols[];

        public Table(String templateName, String localesName, String templateIds[], String localesIds[], String cols[]) {
            this.templateName = templateName;
            this.localesName = localesName;
            this.templateIds = templateIds.clone();
            this.localesIds = localesIds.clone();
            this.cols = cols.clone();
        }
    }

    private final String pwd = "C:\\Users\\Hazi\\Desktop\\wow112\\mydatabase-2.0.8\\_tools";
    //private final String pwd = System.getProperty("user.dir");
    String url = "jdbc:mysql://localhost:3306/mangos";
    String username = "mangos";
    String password = "mangos";
    Connection con = null;
    Statement stmt = null;

    public static void main(String args[]) {
        List<Table> tableList = new LinkedList<Table>();
        //locales_quest
        String templateIds[] = new String[]{"entry"};
        String localesIds[] = new String[]{"entry"};
        String cols[] = new String[]{"Title", "Details", "Objectives", "OfferRewardText", "RequestItemsText", "EndText", "ObjectiveText1", "ObjectiveText2", "ObjectiveText3", "ObjectiveText4"};
        Table table = new Table("quest_template", "locales_quest", templateIds, localesIds, cols);
        tableList.add(table);
        //locales_creature
        templateIds = new String[]{"entry"};
        localesIds = new String[]{"entry"};
        cols = new String[]{"name", "subname"};
        table = new Table("creature_template", "locales_creature", templateIds, localesIds, cols);
        tableList.add(table);
        //locales_item
        templateIds = new String[]{"entry"};
        localesIds = new String[]{"entry"};
        cols = new String[]{"name", "description"};
        table = new Table("item_template", "locales_item", templateIds, localesIds, cols);
        tableList.add(table);
        //locales_gameobject
        templateIds = new String[]{"entry"};
        localesIds = new String[]{"entry"};
        cols = new String[]{"name"};
        table = new Table("gameobject_template", "locales_gameobject", templateIds, localesIds, cols);
        tableList.add(table);
        //locales_gossip_menu_option
        templateIds = new String[]{"menu_id", "id"};
        localesIds = new String[]{"menu_id", "id"};
        cols = new String[]{"option_text", "box_text"};
        table = new Table("gossip_menu_option", "locales_gossip_menu_option", templateIds, localesIds, cols);
        tableList.add(table);
        //locales_page_text
        templateIds = new String[]{"entry"};
        localesIds = new String[]{"entry"};
        cols = new String[]{"text"};
        table = new Table("page_text", "locales_page_text", templateIds, localesIds, cols);
        tableList.add(table);
        //locales_npc_text
        templateIds = new String[]{"ID"};
        localesIds = new String[]{"entry"};
        cols = new String[]{"text0_0", "text0_1", "text1_0", "text1_1", "text2_0", "text2_1", "text3_0", "text3_1", "text4_0", "text4_1", "text5_0", "text5_1", "text6_0", "text6_1", "text7_0", "text7_1"};
        table = new Table("npc_text", "locales_npc_text", templateIds, localesIds, cols);
        tableList.add(table);
        CNDBZero cndb = new CNDBZero();
        cndb.arrange(tableList);
    }

    public void arrange(List<Table> tableList) {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return;
        }
        try {
            con = DriverManager.getConnection(url, username, password);
            con.setAutoCommit(true);
            stmt = con.createStatement();
            String dbpath[] = {pwd + "\\..\\_cn\\cndb\\", pwd + "\\..\\_cn\\acweb\\", pwd + "\\..\\_cn\\vesoo\\"};
            for (Table t : tableList) {
                System.out.println("================\t" + t.localesName + "\t================");
                System.out.println("Creating " + t.localesName + "Tmp");
                sqlCreateTmpTable(t.localesName);
                try {
                    for (String col : t.cols) {
                        sqlClearTable(t.localesName);
                        for (String path : dbpath) {
                            sqlImportCollected(path, t.localesName);
                            sqlDeleteUnfinished(t.localesName, col);
                        }
                        sqlDeleteUnused(t.localesIds, t.templateName, t.localesName);
                        sqlUpdateOriginal(t.localesName, t.localesIds, col);
                    }
                    sqlClearTable(t.localesName);
                    sqlBackUpTable(t.localesName + "Tmp", t.localesName);
                } finally {
                    sqlDropTmpTable(t.localesName);
                }
                System.out.println("================\t" + t.localesName + "\t================\r\n\r\n");
            }

            for (Table t : tableList) {
                for (String col : t.cols) {
                    sqlTranslateMore(t.templateName, t.localesName, col, t.templateIds, t.localesIds);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (null != stmt) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (null != con) {
                try {
                    con.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private int sqlClearTable(String tableName) throws SQLException {
        int re = 0;
        System.out.println("Clearing " + tableName);
        String sql = new StringBuilder("DELETE FROM `").append(tableName).append("`;").toString();
        System.out.println(sql);
        re = stmt.executeUpdate(sql);
        System.out.println(re + " cleared");
        return re;
    }

    private void sqlImportCollected(String path, String localesName) throws IOException, InterruptedException {
        String err = null;
        String cmd[] = {"cmd", "/c", null};
        System.out.println("Importing...");
        cmd[2] = pwd + "\\.\\mysql.exe -umangos -pmangos mangos < " + path + localesName + ".sql --default-character-set=utf8";
        System.out.println("exec:" + cmd[2]);
        Process p = Runtime.getRuntime().exec(cmd);
        p.waitFor();
        BufferedInputStream in = new BufferedInputStream(p.getErrorStream());
        BufferedReader br = new BufferedReader(new InputStreamReader(in, "gbk"));
        while (null != (err = br.readLine())) {
            System.err.println(err);
        }
    }

    private int sqlDeleteUnfinished(String localesName, String col) throws SQLException {
        int re = 0;
        String sql = new StringBuilder("DELETE FROM `").append(localesName).append("` WHERE `").append(col).append("_loc4` IS NULL OR `").append(col).append("_loc4` REGEXP '^[^一-龥]*$'=1;").toString();
        System.out.println(sql);
        re = stmt.executeUpdate(sql);
        return re;
    }

    private int sqlDeleteUnused(String localesIds[], String templateName, String localesName) throws SQLException {
        int re = 0;
        System.out.println("Filtering ...");
        StringBuilder sql = new StringBuilder("DELETE FROM `").append(localesName).append("` WHERE NOT EXISTS (SELECT * FROM `").append(templateName).append("` WHERE `").append(localesName).append("`.`").append(localesIds[0]).append("` = `").append(localesName).append("`.`").append(localesIds[0]).append("` ");
        for (int i = 1; i != localesIds.length; i++) {
            sql.append("AND `").append(localesName).append("`.`").append(localesIds[i]).append("` = `").append(localesName).append("`.`").append(localesIds[i]).append("` ");
        }
        sql.append(");");
        System.out.println(sql.toString());
        re = stmt.executeUpdate(sql.toString());
        return re;
    }

    private int sqlUpdateOriginal(String localesName, String localesIds[], String col) throws SQLException {
        int re = 0;
        StringBuilder sql = new StringBuilder("INSERT INTO `").append(localesName).append("Tmp` (`");
        for (int i = 0; i != localesIds.length; i++) {
            sql.append(localesIds[i]).append("`,`");
        }
        sql.append(col).append("_loc4`) SELECT `");
        for (int i = 0; i != localesIds.length; i++) {
            sql.append(localesIds[i]).append("`,`");
        }
        sql.append(col).append("_loc4` FROM `").append(localesName).append("` ON DUPLICATE KEY UPDATE `").append(localesName).append("Tmp`.`").append(col).append("_loc4` = `").append(localesName).append("`.`").append(col).append("_loc4`;");
        System.out.println(sql);
        re = stmt.executeUpdate(sql.toString());
        System.out.println(re + " updated");
        return re;
    }

    private void sqlCreateTmpTable(String localesName) throws SQLException {
        String sql = new StringBuilder("CREATE TABLE `").append(localesName).append("Tmp` LIKE `").append(localesName).append("`;").toString();
        System.out.println(sql);
        stmt.executeUpdate(sql);
    }

    private void sqlDropTmpTable(String localesName) throws SQLException {
        String sql = new StringBuilder("DROP TABLE `").append(localesName).append("Tmp`;").toString();
        System.out.println(sql);
        stmt.executeUpdate(sql);
    }

    private void sqlBackUpTable(String srcTable, String destTable) throws SQLException {
        String sql = new StringBuilder("INSERT INTO `").append(destTable).append("` SELECT * FROM `").append(srcTable).append("`;").toString();
        System.out.println(sql);
        stmt.executeUpdate(sql);
    }

    //INSERT INTO `locales_quest` (`entry`,`Details_loc4`) (SELECT `ic1` AS `id1`,`Details` FROM (SELECT `ib1` AS `ic1`,`Details_loc4` AS `Details` FROM `locales_quest` `t1` RIGHT JOIN (SELECT `entry` AS `ib1`,`va2` AS `vb1` FROM `locales_quest` `t1` RIGHT JOIN (SELECT `t1`.`entry` AS `ia1`,`t2`.`entry` AS `va2` FROM `quest_template` AS `t1`,`quest_template` AS `t2` WHERE `t1`.`Details` <> '' AND `t1`.`entry` <> `t2`.`entry` AND `t1`.`Details` = `t2`.`Details`) `t2` ON `t1`.`entry` = `t2`.`ia1` WHERE `Details_loc4` IS NULL) `t2` ON `t1`.`entry` = `t2`.`vb1` AND `Details_loc4` IS NOT NULL) AS `t`) ON DUPLICATE KEY UPDATE `Details_loc4` = `Details`;
    private int sqlTranslateMore(String templateName, String localesName, String col, String templateIds[], String localesIds[]) throws SQLException {
        StringBuilder sql = new StringBuilder("INSERT INTO `").append(localesName).append("` (`");
        for (int i = 0; i != localesIds.length; i++) {
            sql.append(localesIds[i]).append("`,`");
        }
        sql.append(col).append("_loc4`) (SELECT `");
        for (int i = 0; i != localesIds.length; i++) {
            sql.append("ic").append(i + 1).append("` AS `id").append(i + 1).append("`,`");
        }
        sql.append(col).append("` FROM (SELECT `");
        for (int i = 0; i != localesIds.length; i++) {
            sql.append("ib").append(i + 1).append("` AS `ic").append(i + 1).append("`,`");
        }
        sql.append(col).append("_loc4` AS `").append(col).append("` FROM `").append(localesName).append("` `t1` RIGHT JOIN (SELECT `");
        for (int i = 0; i != localesIds.length; i++) {
            sql.append("ia").append(i + 1).append("` AS `ib").append(i + 1).append("`,`");
        }
        for (int i = 0; i != localesIds.length; i++) {
            sql.append("va").append(i + 1).append("` AS `vb").append(i + 1).append("`,`");
        }
        sql = new StringBuilder(sql.substring(0, sql.length() - 2));
        sql.append(" FROM `").append(localesName).append("` `t1` RIGHT JOIN (SELECT ");
        for (int i = 0; i != templateIds.length; i++) {
            sql.append("`t1`.`").append(templateIds[i]).append("` AS `ia").append(i + 1).append("`,");
        }
        sql = new StringBuilder(sql.substring(0, sql.length() - 1));
        for (int i = 0; i != templateIds.length; i++) {
            sql.append(",`t2`.`").append(templateIds[i]).append("` AS `va").append(i + 1).append("`");
        }
        sql.append(" FROM `").append(templateName).append("` AS `t1`,`").append(templateName).append("` AS `t2` WHERE `t1`.`").append(col).append("` <> '' AND `t1`.`").append(col).append("` IS NOT NULL AND (");
        for (int i = 0; i != templateIds.length; i++) {
            sql.append("`t1`.`").append(templateIds[i]).append("` <> `t2`.`").append(templateIds[i]).append("` OR ");
        }
        sql = new StringBuilder(sql.substring(0, sql.length() - 3));
        sql.append(") AND `t1`.`").append(col).append("` = `t2`.`").append(col).append("`) `t2` ON ");
        for (int i = 0; i != localesIds.length; i++) {
            sql.append("`t1`.`").append(localesIds[i]).append("` = `").append("t2`.`ia").append(i + 1).append("` AND ");
        }
        sql = new StringBuilder(sql.substring(0, sql.length() - 4));
        sql.append(" WHERE `").append(col).append("_loc4` IS NULL) `t2` ON `");
        for (int i = 0; i != localesIds.length; i++) {
            sql.append("t1`.`").append(localesIds[i]).append("` = `t2`.`vb").append(i + 1).append("` AND `");
        }
        sql = new StringBuilder(sql.substring(0, sql.length() - 5)).append("WHERE `");
        sql.append(col).append("_loc4` IS NOT NULL) AS `t`) ON DUPLICATE KEY UPDATE `").append(col).append("_loc4` = `").append(col).append("`;");
        System.out.println(sql.toString());
        int re = stmt.executeUpdate(sql.toString());
        System.out.println(re + " more got");
        return re;
    }
}
