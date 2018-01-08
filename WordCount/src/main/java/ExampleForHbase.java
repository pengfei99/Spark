import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import java.io.IOException;

public class ExampleForHbase {
    public static Configuration configuration;
    public static Connection connect;
    public static Admin admin;

    public static void main(String[] args) throws IOException {
        //equivalent shell command: create 'Score','sname','course'
        //createTable("Score",new String[]{"sname","course"});
        //Insert a row into table Score，with row key value 95001, the column family is sname, the value is Mary
        //（Because sname column family does not have any child, so col parameter is empty）
        // equivalent shell command：put 'Score','95001','sname','Mary'
        insertRow("Score", "95001", "sname", "", "Mary");
        //Insert a row with row key 95001, column Math of column family course (course:Math), the row value is 88
        //equivalent shell command：put 'Score','95001','score:Math','88'
        insertRow("Score", "95001", "course", "Math", "88");

        //Insert another row as：put 'Score','95001','score:English','85'
        insertRow("Score", "95001", "course", "English", "85");

        //1、delete row of column score:Math with rowkey 95001
        //equivalent shell command：delete 'Score','95001','score:Math'
        deleteRow("Score", "95002", "course", "Math");

        //2、delete row of column family course, the column(e.g. math, english) under column family course will be all deleted
        //equivalent shell command：delete 'Score','95001','course'
        //deleteRow("Score", "95001", "course", "");

        //3、delete row with row key 95001 (all column family data will be removed)
        //eqivalent command in shell：deleteall 'Score','95001'
        //deleteRow("Score", "95001", "", "");

        //get data of row with row key 95001 of the column course:Math
        //getData("Score", "95001", "course", "Math");
        //get data of row with row key 95001 of column family sname
        //getData("Score", "95001", "sname", "");

        //delete table
        //deleteTable("Score");

        // close connection
        close();
    }

    /**
* Create a table in the HBase server
* @param tableName is the name of the table you are creating
* @param colFamily is a list of column families names
* @throws IOException
* */
    private static void createTable(String tableName, String[] colFamily) throws IOException {
        init();
        TableName tabName = TableName.valueOf(tableName);
        if(admin.tableExists(tabName)){
            System.out.println("Table name already exists");
        }else{
            //The constructor with String as table name is deprecated
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tabName);
        for (String str:colFamily){
            HColumnDescriptor hColumnDescriptor=new HColumnDescriptor(tableName);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        admin.createTable(hTableDescriptor);
            System.out.println("Create table success");
        }
    }

    //This method set up a connection to Hbase server
    private static void init(){
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.rootdir","hdfs://localhost:9000/hbase");

        try {
            connect=ConnectionFactory.createConnection(configuration);
            admin = connect.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    //This method close the connection to Hbase server
    private static void close(){
        try{
            if(admin != null){
                admin.close();
            }
            if(null != connect){
                connect.close();
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * This method deletes a table with the given name
     * @param tableName
     * @throws IOException
     */
    public static void deleteTable(String tableName) throws IOException {
        init();
        TableName tn = TableName.valueOf(tableName);
        if (admin.tableExists(tn)) {
            admin.disableTable(tn);
            admin.deleteTable(tn);
        }
        close();
    }

    /**
     * show existing table
     * @throws IOException
     */
    public static void listTables() throws IOException {
        init();
        HTableDescriptor hTableDescriptors[] = admin.listTables();
        for(HTableDescriptor hTableDescriptor :hTableDescriptors){
            System.out.println(hTableDescriptor.getNameAsString());
        }
        close();
    }
    /**
     * insert a row into a column of a columnFamily with given table name
     * @param tableName specify which table you want to insert the row
     * @param rowKey specify the key id of a row
     * @param colFamily column family name
     * @param col column name, it can be empty if the colFamily doesn't have any column
     * @param val row value which we want to insert.
     * @throws IOException
     */
    public static void insertRow(String tableName,String rowKey,String colFamily,String col,String val) throws IOException {
        init();
        Table table = connect.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(colFamily.getBytes(), col.getBytes(), val.getBytes());
        table.put(put);
        table.close();
        close();
    }

    /**
     * delete a row value of a column in a column family
     * @param tableName
     * @param rowKey
     * @param colFamily
     * @param col
     * @throws IOException
     */
    public static void deleteRow(String tableName,String rowKey,String colFamily,String col) throws IOException {
        init();
        Table table = connect.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(rowKey.getBytes());
        //delete all data in one column family
        delete.addFamily(colFamily.getBytes());
        //delete all data in one column of one column family
        delete.addColumn(colFamily.getBytes(), col.getBytes());

        table.delete(delete);
        table.close();
        close();
    }
    /**
     * get value of one row based on its row key
     * @param tableName
     * @param rowKey
     * @param colFamily
     * @param col
     * @throws IOException
     */
    public static void getData(String tableName,String rowKey,String colFamily,String col)throws  IOException{
        init();
        Table table = connect.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        get.addColumn(colFamily.getBytes(),col.getBytes());
        Result result = table.get(get);
        showCell(result);
        table.close();
        close();
    }
    /**
     *
     * @param result
     */
    public static void showCell(Result result){
        Cell[] cells = result.rawCells();
        for(Cell cell:cells){
            System.out.println("RowName:"+new String(CellUtil.cloneRow(cell))+" ");
            System.out.println("Timetamp:"+cell.getTimestamp()+" ");
            System.out.println("column Family:"+new String(CellUtil.cloneFamily(cell))+" ");
            System.out.println("column Name:"+new String(CellUtil.cloneQualifier(cell))+" ");
            System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");
        }
    }
}
