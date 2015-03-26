import java.sql.*;

import oracle.sql.STRUCT;
import oracle.spatial.geometry.JGeometry;

public class HW2 {
	private Connection conn = null;
	private final String connection;
	private final String username;
	private final String password;
	private Statement stmt = null;
	private ResultSet rs = null;
	static String query, tableName, stu_id;
	//JSDOGeometry geom = null;
	
	HW2(String username, String password, String connection){
		this.username = username;
		this.password = password;
		this.connection = connection;
	}
	
	void getDBConnection(){
		try{
			DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
		}catch(SQLException ex){
			System.out.println("Please install Oracle Driver.");
			return;
		}
		try{
			conn = DriverManager.getConnection(connection,username,password);
		}catch(SQLException e){
			System.out.println(e);
			return;
		}
		if (conn != null){
			System.out.println("Connection Succeeded.");
		}else{
			System.out.println("Connection failed.");
		}
		
	}
	
	void getWindowResult(String tableName, String lx, String ly, String ux, String uy){
		try{
			/*String pk = "";
			DatabaseMetaData dbMeta = conn.getMetaData();
			ResultSet pkRSet = dbMeta.getPrimaryKeys(null, null, tableName);
			//System.out.println(pkRSet);
			while (pkRSet.next()){
				//System.out.println("pk0"+pk);
				pk = pkRSet.getString("PK_NAME");
			}
			System.out.println("pk"+pk);*/
			String sql = "select * from " + tableName + " where sdo_relate(" + tableName + ".shape, SDO_geometry(2003,NULL,NULL,SDO_elem_info_array(1,1003,3),SDO_ordinate_array("+lx+","+ly+","+ux+","+uy+")),'mask=anyinteract') = 'TRUE'";
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			while(rs.next()){
				System.out.print(rs.getString(1)+" ");
            }
			closeAll();
		}catch(SQLException e){
			System.out.println(e);
			return;
		}
	}
	
	void getWithinResult(String stu_id, String distance){
		try{
			String sql = "select bldg_id as bldg_stop_id from building where SDO_WITHIN_DISTANCE(building.shape,(select s.shape from student s where s.stu_id = '" 
						+ stu_id + "')"+",'distance=" + distance 
						+ "') = 'TRUE' UNION select t.stop_id as bldg_stop_id from tramstop t where SDO_WITHIN_DISTANCE(t.shape,(select s.shape from student s where s.stu_id = '"
						+ stu_id + "')"+",'distance=" + distance + "') = 'TRUE'";
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			System.out.println("BLDG_STOP_ID");
			System.out.println("------------------");
			while (rs.next()){
				System.out.println(rs.getString("bldg_stop_id"));
			}
			closeAll();
		}catch(SQLException e){
			System.out.println(e);
			return;
		}
	}
	
	void getNNResult(String object, String id, String num){
		try{
			if (object.equals("building")){
				if (id.charAt(0) != 'b'){
					System.out.println("The ID you entered cannot match the object you gave.");
				}else{
					String sql = "SELECT b.bldg_id FROM building b WHERE SDO_NN(b.shape,(SELECT c.shape FROM building c WHERE c.bldg_id = '" + id + "'),  'sdo_num_res=" + Integer.parseInt(num) + "') = 'TRUE'";
					stmt = conn.createStatement();
					rs = stmt.executeQuery(sql);
					System.out.println("BLDG_ID");
					System.out.println("------------------");
					while (rs.next()){
						System.out.println(rs.getString("bldg_id"));
					}
					closeAll();
				}
			}
			if (object.equals("student")){
				if (id.charAt(0) != 'p'){
					System.out.println("The ID you entered cannot match the object you gave.");
				}else{
					String sql = "SELECT s.stu_id FROM student s WHERE SDO_NN(s.shape,(SELECT s.shape FROM student s WHERE s.stu_id = '" + id + "'),  'sdo_num_res=" + Integer.parseInt(num) + "') = 'TRUE'";
					stmt = conn.createStatement();
					rs = stmt.executeQuery(sql);
					System.out.println("STU_ID");
					System.out.println("------------------");
					while (rs.next()){
						System.out.println(rs.getString("stu_id"));
					}
					closeAll();
				}
			}
			if (object.equals("tramstop")){
				if (id.charAt(0) != 't'){
					System.out.println("The ID you entered cannot match the object you gave.");
				}else{
					String sql = "SELECT t.stop_id FROM tramstop t WHERE SDO_NN(t.shape,(SELECT t.shape FROM tramstop t WHERE t.stop_id = '" + id + "'),  'sdo_num_res=" + Integer.parseInt(num) + "') = 'TRUE'";
					stmt = conn.createStatement();
					rs = stmt.executeQuery(sql);
					System.out.println("STOP_ID");
					System.out.println("------------------");
					while (rs.next()){
						System.out.println(rs.getString("stop_id"));
					}
					closeAll();
				}
			}
		}catch(SQLException e){
			System.out.println(e);
			return;
		}
	}
	
	void getFixedResult(String queryNum){
		try{
			if (queryNum.equals("1")){
				String stop1 = "SDO_GEOMETRY(2003, NULL, NULL,SDO_ELEM_INFO_ARRAY(1,1003,4),SDO_ORDINATE_ARRAY(204,247, 274,177, 204,107))) = 'TRUE'";
				String stop2 = "SDO_GEOMETRY(2003, NULL, NULL,SDO_ELEM_INFO_ARRAY(1,1003,4),SDO_ORDINATE_ARRAY(213,482, 263,432, 213,382))) = 'TRUE'";
				String sql = "SELECT s.stu_id AS all_id FROM student s WHERE SDO_INSIDE(s.shape, " + stop1 + " AND SDO_INSIDE(s.shape, " + stop2
							+ " UNION " + "SELECT b.bldg_id AS all_id FROM building b WHERE SDO_INSIDE(b.shape, " + stop1 + " AND SDO_INSIDE(b.shape, " + stop2;
				stmt = conn.createStatement();
				rs = stmt.executeQuery(sql);
				System.out.println("ALL_ID");
				System.out.println("------------------");
				while (rs.next()){
					System.out.println(rs.getString("all_id"));
				}
				closeAll();
			}
			if (queryNum.equals("2")){
				String sql = "select max(rk) as rowcount from (select rownum as rk from student) t";
				stmt = conn.createStatement();
				rs = stmt.executeQuery(sql);
				rs.next();
				String rowCount = rs.getString("rowcount");
				for (int i = 0; i < Integer.parseInt(rowCount); i++){
					sql = "SELECT t.stop_id, ROUND(SDO_NN_DISTANCE(1),2) AS dist FROM tramstop t WHERE SDO_NN(t.shape,(SELECT s.shape FROM student s WHERE s.stu_id = 'p" 
							+ i + "'),  'sdo_num_res=2', 1) = 'TRUE'";
					//stmt = conn.createStatement();
					rs = stmt.executeQuery(sql);
					while (rs.next()){
						System.out.println("p"+i+": "+rs.getString("stop_id")+"  "+ rs.getString("dist"));
					}
				}
				closeAll();
			}
			if (queryNum.equals("3")){
				int tramid = 0;
				String sql = "SELECT MAX(rk) AS rowcount FROM (SELECT rownum AS rk FROM tramstop) t";
				//System.out.println(sql);
				stmt = conn.createStatement();
				rs = stmt.executeQuery(sql);
				rs.next();
				String rowCount = rs.getString("rowcount");
				int[] arr = new int[Integer.parseInt(rowCount)];
				for (int i = 0; i < Integer.parseInt(rowCount); i++){
					sql = "SELECT b.bldg_id FROM building b WHERE SDO_WITHIN_DISTANCE(b.shape,(SELECT t.shape FROM (SELECT rownum id,tramstop.* FROM tramstop) t WHERE t.id = " + (i+1)+"),'distance=250') = 'TRUE'";
					//System.out.println(sql);
					//stmt = conn.createStatement();
					rs = stmt.executeQuery(sql);
					int j = 0;
					while (rs.next()){
						j++;
					}
					arr[i] = j;
					//System.out.println("tramstop#"+i+"has "+j+" buildings within the distance 250");
				}
				int max = arr[0];
				for (int k = 0; k < arr.length; k++){
					if (arr[k] > max){
						max = arr[k];
						tramid = k;
					}
				}
				String[] stopIDs = {"t1psa","t2ohe","t3sgm","t4hnb","t5vhe","t6ssl","t7helen"};
				System.out.println("tramstop " + stopIDs[tramid] + " covers the most buildings which is " + arr[tramid]);
				closeAll();
			}
			if (queryNum.equals("4")){
				int stuid = 0;
				String sql = "SELECT MAX(rk) AS rowcount FROM (SELECT rownum AS rk FROM building) t";
				stmt = conn.createStatement();
				rs = stmt.executeQuery(sql);
				rs.next();
				String rowCount = rs.getString("rowcount");
				int[] arr = new int[100];
				for (int i = 0; i < Integer.parseInt(rowCount); i++){
					sql = "SELECT s.stu_id FROM student s WHERE SDO_NN(s.shape,(SELECT b.shape FROM building b WHERE b.bldg_id = 'b" + i + "'),'sdo_num_res=1') = 'TRUE'";
					//stmt = conn.createStatement();
					rs = stmt.executeQuery(sql);
					while (rs.next()){
						String stu_num = rs.getString("stu_id").substring(1);
						arr[Integer.parseInt(stu_num)] += 1;
					}
				}
				int max = arr[0];
				for (int j = 0; j < 5; j++){
					for (int k = 0; k < arr.length; k++){
						if (arr[k] > max){
							max = arr[k];
							stuid = k;
						}
					}
					System.out.println("p" + stuid + ": " + max);
					arr[stuid] = 0; max = arr[0];
				}
				closeAll();
			}
			if (queryNum.equals("5")){
				String sql = "SELECT SDO_AGGR_MBR(b.shape) FROM building b WHERE b.bldg_name like 'SS%'";
				//System.out.println(sql);
				stmt = conn.createStatement();
				rs = stmt.executeQuery(sql);
				while (rs.next()){
					STRUCT strt = (oracle.sql.STRUCT) rs.getObject(1);
		            JGeometry jGeo = JGeometry.load(strt);
		            double[] a= jGeo.getMBR();
		            System.out.println("("+a[0]+","+a[1]+")");
		            System.out.println("("+a[2]+","+a[3]+")");
				}
				closeAll();
			}
		}catch(SQLException e){
			System.out.println(e);
			return;
		}
	}
	
	void closeAll(){
		try{
			rs.close();
			stmt.close();
			conn.close();
		}catch(SQLException e){
			System.out.println(e);
			return;
		}
	}
	
	public static void main(String[] args){
		if (args.length == 0) System.out.println("Please enter query type, objecty type or fixed number.");
		else{
			query = args[0];
			HW2 obj = new HW2("bingxin","bingxin","jdbc:oracle:thin:@localhost:1521:myorcl");
			obj.getDBConnection();
			if (query.equals("window")){
				tableName = args[1];
				obj.getWindowResult(tableName,args[2], args[3], args[4], args[5]);
			}
			if (query.equals("within")){
				stu_id = args[1];
				obj.getWithinResult(stu_id,args[2]);
			}
			if (query.equals("nearest-neighbor")){
				String object = args[1];
				obj.getNNResult(object,args[2],args[3]);
			}
			if (query.equals("fixed")){
				String queryNum = args[1];
				obj.getFixedResult(queryNum);
			}
			System.out.println("Test Exit.");
		}
	}
}
