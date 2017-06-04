

import java.io.RandomAccessFile;
import java.io.FileReader;
import java.io.File;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;
import java.text.SimpleDateFormat;

public class Table{
	public static final int pageSize = 512;
	public static final String datePattern = "yyyy-MM-dd_HH:mm:ss";
	private static RandomAccessFile davisbaseTablesCatalog;
	private static RandomAccessFile davisbaseColumnsCatalog;

	
	public static void main(String[] args){}

	
	public static void show(){
		String[] cols = {"table_name"};
		String[] cmp = new String[0];
		String table = "davisbase_tables";
		select(table, cols, cmp);
	}

	
	public static void drop(String table){
		try{
			
			RandomAccessFile file = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
			int nPages = pages(file);
			for(int page = 1; page <= nPages; page ++){
				file.seek((page-1)*pageSize);
				byte type = file.readByte();
				if(type == 0x05)
					continue;
				else{
					short[] cells = Page.getCellArray(file, page);
					int i = 0;
					for(int j = 0; j < cells.length; j++){
						long loc = Page.getCellLoc(file, page, j);
						String[] pl = retrievePayload(file, loc);
						String tb = pl[1];
						if(!tb.equals(table)){
							Page.setCellOffset(file, page, i, cells[j]);
							i++;
						}
					}
					Page.setCellNumber(file, page, (byte)i);
				}
			}

			
			file = new RandomAccessFile("data/user_data/davisbase_columns.tbl", "rw");
			nPages = pages(file);
			for(int page = 1; page <= nPages; page ++){
				file.seek((page-1)*pageSize);
				byte type = file.readByte();
				if(type == 0x05)
					continue;
				else{
					short[] cells = Page.getCellArray(file, page);
					int i = 0;
					for(int j = 0; j < cells.length; j++){
						long loc = Page.getCellLoc(file, page, j);
						String[] pl = retrievePayload(file, loc);
						String tb = pl[1];
						if(!tb.equals(table)){
							Page.setCellOffset(file, page, i, cells[j]);
							i++;
						}
					}
					Page.setCellNumber(file, page, (byte)i);
				}
			}

			
			File prevFile = new File("data", table+".tbl"); 
			prevFile.delete();
		}catch(Exception e){
			System.out.println("Error at drop");
			System.out.println(e);
		}

	}

	public static String[] retrievePayload(RandomAccessFile file, long loc){
		String[] payload = new String[0];
		try{
			Long tmp;
			SimpleDateFormat formater = new SimpleDateFormat (datePattern);

			
			file.seek(loc);
			int plsize = file.readShort();
			int key = file.readInt();
			int num_cols = file.readByte();
			byte[] stc = new byte[num_cols];
			int temp = file.read(stc);
			payload = new String[num_cols+1];
			payload[0] = Integer.toString(key);
			
			for(int i=1; i <= num_cols; i++){
				switch(stc[i-1]){
					case 0x00:  payload[i] = Integer.toString(file.readByte());
								payload[i] = "null";
								break;

					case 0x01:  payload[i] = Integer.toString(file.readShort());
								payload[i] = "null";
								break;

					case 0x02:  payload[i] = Integer.toString(file.readInt());
								payload[i] = "null";
								break;

					case 0x03:  payload[i] = Long.toString(file.readLong());
								payload[i] = "null";
								break;

					case 0x04:  payload[i] = Integer.toString(file.readByte());
								break;

					case 0x05:  payload[i] = Integer.toString(file.readShort());
								break;

					case 0x06:  payload[i] = Integer.toString(file.readInt());
								break;

					case 0x07:  payload[i] = Long.toString(file.readLong());
								break;

					case 0x08:  payload[i] = String.valueOf(file.readFloat());
								break;

					case 0x09:  payload[i] = String.valueOf(file.readDouble());
								break;

					case 0x0A:  tmp = file.readLong();
								Date dateTime = new Date(tmp);
								payload[i] = formater.format(dateTime);
								break;

					case 0x0B:  tmp = file.readLong();
								Date date = new Date(tmp);
								payload[i] = formater.format(date).substring(0,10);
								break;

					default:    int len = new Integer(stc[i-1]-0x0C);
								byte[] bytes = new byte[len];
								for(int j = 0; j < len; j++)
									bytes[j] = file.readByte();
								payload[i] = new String(bytes);
								break;
				}
			}

		}catch(Exception e){
			System.out.println("Error at retrievePayload");
		}

		return payload;
	}


	public static void createTable(String table, String[] col){
		
		try{	
			
			RandomAccessFile file = new RandomAccessFile("data/user_data/"+table+".tbl", "rw");
			file.setLength(pageSize);
			file.seek(0);
			file.writeByte(0x0D);
			file.close();
            file = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
			
			int numPages = pages(file);
			int page = 1;
			for(int p = 1; p <= numPages; p++){
				int rm = Page.getRightMost(file, p);
				if(rm == 0)
			 		page = p;
			}
			int[] keyArray = Page.getKeyArray(file, page);
			int l = keyArray[0];
			for(int i = 0; i < keyArray.length; i++)
				if(l < keyArray[i])
					l = keyArray[i];
			file.close();
			String[] values = {Integer.toString(l+1), table};
			insertInto("davisbase_tables", values);

			RandomAccessFile cfile = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
			Cache cache = new Cache();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] cmp = {};
			filter(cfile, cmp, columnName, cache);
			l = cache.content.size();

			for(int i = 0; i < col.length; i++){
				l = l + 1;
				String[] token = col[i].split(" ");
				String n = "YES";
				if(token.length > 2)
					n = "NO";
				String col_name = token[0];
				String dt = token[1].toUpperCase();
				String pos = Integer.toString(i+1);
				String[] v = {Integer.toString(l), table, col_name, dt, pos, n};
				insertInto("davisbase_columns", v);
			}
			file.close();
		}catch(Exception e){
			System.out.println("Error at createTable");
			e.printStackTrace();
		}
	}

	public static void update(String table, String[] set, String[] cmp){
		try{
			int key = new Integer(cmp[2]);
			
			RandomAccessFile file=null;
			if(table.equals("davisbase_tables") || table.equals("davisbase_columns")){
				file=new RandomAccessFile("data/catalog/"+table+".tbl", "rw");
			}
			else
			{
				file=new RandomAccessFile("data/user_data/"+table+".tbl", "rw");
			}
			
			int numPages = pages(file);
			int page = 1;

			for(int p = 1; p <= numPages; p++)
				if(Page.hasKey(file, p, key)){
					page = p;
				}
			int[] array = Page.getKeyArray(file, page);
			int id = 0;
			for(int i = 0; i < array.length; i++)
				if(array[i] == key)
					id = i;
			int offset = Page.getCellOffset(file, page, id);
			long loc = Page.getCellLoc(file, page, id);
			String[] array_s = getColName(table);
			int num_cols = array_s.length - 1;
			String[] values = retrievePayload(file, loc);
			String[] type = getDataType(table);
			for(int i=0; i < type.length; i++)
				if(type[i].equals("DATE") || type[i].equals("DATETIME"))
					values[i] = "'"+values[i]+"'";


			
			for(int i = 0; i < array_s.length; i++)
				if(array_s[i].equals(set[0]))
					id = i;
			values[id] = set[2];

	
			String[] nullable = getNullable(table);

			for(int i = 0; i < nullable.length; i++){
				if(values[i].equals("null") && nullable[i].equals("NO")){
					System.out.println("NULL value constraint violation");
					System.out.println();
					return;
				}
			}
            byte[] stc = new byte[array_s.length-1];
			int plsize = calPayloadSize(table, values, stc);
			Page.updateLeafCell(file, page, offset, plsize, key, stc, values);

			file.close();

		}catch(Exception e){
			System.out.println("Error at update");
			System.out.println(e);
		}
	}

	public static void insertInto(RandomAccessFile file, String table, String[] values){
		String[] dtype = getDataType(table);
		String[] nullable = getNullable(table);

		for(int i = 0; i < nullable.length; i++)
			if(values[i].equals("null") && nullable[i].equals("NO")){
				System.out.println("NULL value constraint violation");
				System.out.println();
				return;
			}


		int key = new Integer(values[0]);
		int page = searchKey(file, key);
		if(page != 0)
			if(Page.hasKey(file, page, key)){
				System.out.println("Uniqueness constraint violation");
				System.out.println();
				return;
			}
		if(page == 0)
			page = 1;


		byte[] stc = new byte[dtype.length-1];
		short plSize = (short) calPayloadSize(table, values, stc);
		int cellSize = plSize + 6;
		int offset = Page.checkLeafSpace(file, page, cellSize);


		if(offset != -1){
			Page.insertLeafCell(file, page, offset, plSize, key, stc, values);
			
		}else{
			Page.splitLeaf(file, page);
			insertInto(file, table, values);
		}
	}
	
	
	public static void delInto(RandomAccessFile file, String table, String[] values){
		String[] dtype = getDataType(table);
		String[] nullable = getNullable(table);

		for(int i = 0; i < nullable.length; i++)
			if(values[i].equals("null") && nullable[i].equals("NO")){
				System.out.println("NULL value constraint violation");
				System.out.println();
				return;
			}


		int key = new Integer(values[0]);
		int page = searchKey(file, key);
		if(page != 0)
			if(Page.hasKey(file, page, key)){
				System.out.println("Uniqueness constraint violation");
				System.out.println();
				return;
			}
		if(page == 0)
			page = 1;


		byte[] stc = new byte[dtype.length-1];
		short plSize = (short) calPayloadSize(table, values, stc);
		int cellSize = plSize + 6;
		int offset = Page.checkLeafSpace(file, page, cellSize);


		if(offset != -1){
			Page.deleteLeafCell(file, page, offset, plSize, key, stc, values);
			
		}else
		
		{
			Page.splitLeaf(file, page);
			insertInto(file, table, values);
		}
	}
	

	public static void insertInto(String table, String[] values){
		RandomAccessFile file=null;
		
		try{
			
			if(table.equals("davisbase_tables") || table.equals("davisbase_columns")){
				file=new RandomAccessFile("data/catalog/"+table+".tbl", "rw");
				
				insertInto(file, table, values);
				file.close();
			}
			else
			{
				file=new RandomAccessFile("data/user_data/"+table+".tbl", "rw");
				
				insertInto(file, table, values);
				file.close();
			}
		}catch(Exception e){
			System.out.println("Error at insertInto table");
			e.printStackTrace();
		}
	}

	
	public static int calPayloadSize(String table, String[] vals, byte[] stc){
		String[] dataType = getDataType(table);
		int size = 1;
		size = size + dataType.length - 1;
		for(int i = 1; i < dataType.length; i++){
			byte tmp = stcCode(vals[i], dataType[i]);
			stc[i - 1] = tmp;
			size = size + feildLength(tmp);
		}
		return size;
	}

	
	public static short feildLength(byte stc){
		switch(stc){
			case 0x00: return 1;
			case 0x01: return 2;
			case 0x02: return 4;
			case 0x03: return 8;
			case 0x04: return 1;
			case 0x05: return 2;
			case 0x06: return 4;
			case 0x07: return 8;
			case 0x08: return 4;
			case 0x09: return 8;
			case 0x0A: return 8;
			case 0x0B: return 8;
			default:   return (short)(stc - 0x0C);
		}
	}

	
	public static byte stcCode(String val, String dataType){
		if(val.equals("null")){
			switch(dataType){
				case "TINYINT":     return 0x00;
				case "SMALLINT":    return 0x01;
				case "INT":			return 0x02;
				case "BIGINT":      return 0x03;
				case "REAL":        return 0x02;
				case "DOUBLE":      return 0x03;
				case "DATETIME":    return 0x03;
				case "DATE":        return 0x03;
				case "TEXT":        return 0x03;
				default:			return 0x00;
			}							
		}else
		
		{
			switch(dataType){
				case "TINYINT":     return 0x04;
				case "SMALLINT":    return 0x05;
				case "INT":			return 0x06;
				case "BIGINT":      return 0x07;
				case "REAL":        return 0x08;
				case "DOUBLE":      return 0x09;
				case "DATETIME":    return 0x0A;
				case "DATE":        return 0x0B;
				case "TEXT":        return (byte)(val.length()+0x0C);
				default:			return 0x00;
			}
		}
	}

	public static int searchKey(RandomAccessFile file, int key){
		int val = 1;
		try{
			int numPages = pages(file);
			for(int page = 1; page <= numPages; page++){
				file.seek((page - 1)*pageSize);
				byte pageType = file.readByte();
				if(pageType == 0x0D){
					int[] keys = Page.getKeyArray(file, page);
					if(keys.length == 0)
						return 0;
					int rm = Page.getRightMost(file, page);
					if(keys[0] <= key && key <= keys[keys.length - 1]){
						return page;
					}else if(rm == 0 && keys[keys.length - 1] < key){
						return page;
					}
				}
			}
		}catch(Exception e){
			System.out.println("Error at searchKey");
			System.out.println(e);
		}

		return val;
	}


	public static String[] getDataType(String table){
		String[] dataType = new String[0];
		try{
			RandomAccessFile file = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
			Cache cache = new Cache();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] cmp = {"table_name","=",table};
			filter(file, cmp, columnName, cache);
			HashMap<Integer, String[]> content = cache.content;
			ArrayList<String> array = new ArrayList<String>();
			for(String[] i : content.values()){
				array.add(i[3]);
			}
			dataType = array.toArray(new String[array.size()]);
			file.close();
			return dataType;
		}catch(Exception e){
			System.out.println("Error at getDataType");
			System.out.println(e);
		}
		return dataType;
	}

	public static String[] getColName(String table){
		String[] c = new String[0];
		try{
			RandomAccessFile file = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
			Cache cache = new Cache();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] cmp = {"table_name","=",table};
			filter(file, cmp, columnName, cache);
			HashMap<Integer, String[]> content = cache.content;
			ArrayList<String> array = new ArrayList<String>();
			for(String[] i : content.values()){
				array.add(i[2]);
			}
			c = array.toArray(new String[array.size()]);
			file.close();
			return c;
		}catch(Exception e){
			System.out.println("Error at getColName");
			System.out.println(e);
		}
		return c;
	}

	public static String[] getNullable(String table){
		String[] n = new String[0];
		try{
			RandomAccessFile file = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
			Cache cache = new Cache();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] cmp = {"table_name","=",table};
			filter(file, cmp, columnName, cache);
			HashMap<Integer, String[]> content = cache.content;
			ArrayList<String> array = new ArrayList<String>();
			for(String[] i : content.values()){
				array.add(i[5]);
			}
			n = array.toArray(new String[array.size()]);
			file.close();
			return n;
		}catch(Exception e){
			System.out.println("Error at getNullable");
			System.out.println(e);
		}
		return n;
	}

	public static void select(String table, String[] cols, String[] cmp){
		try{
			Cache cache = new Cache();
			RandomAccessFile file=null;
			if(table.equals("davisbase_tables") || table.equals("davisbase_columns")){
				file=new RandomAccessFile("data/catalog/"+table+".tbl", "rw");
			}
			else
			{
				file=new RandomAccessFile("data/user_data/"+table+".tbl", "rw");
			}
			String[] columnName = getColName(table);
			String[] type = getDataType(table);
			filter(file, cmp, columnName, type, cache);
			cache.display(cols,table);
			file.close();
		}catch(Exception e){
			System.out.println("Error at select");
			System.out.println(e);
		}
	}

	
	public static void filter(RandomAccessFile file, String[] cmp, String[] columnName, String[] type, Cache cache){
		try{
			int numPages = pages(file);
			
			for(int page = 1; page <= numPages; page++){
				file.seek((page-1)*pageSize);
				byte pageType = file.readByte();
				if(pageType == 0x05)
					continue;
				else{
					byte numCells = Page.getCellNumber(file, page);

					for(int i=0; i < numCells; i++){
						
						long loc = Page.getCellLoc(file, page, i);
						file.seek(loc+2); 
						int rowid = file.readInt(); 
						int num_cols = new Integer(file.readByte()); 

						String[] payload = retrievePayload(file, loc);

						for(int j=0; j < type.length; j++)
							if(type[j].equals("DATE") || type[j].equals("DATETIME"))
								payload[j] = "'"+payload[j]+"'";
						
						boolean check = cmpCheck(payload, rowid, cmp, columnName);

						for(int j=0; j < type.length; j++)
							if(type[j].equals("DATE") || type[j].equals("DATETIME"))
								payload[j] = payload[j].substring(1, payload[j].length()-1);

						if(check)
							cache.add(rowid, payload);
					}
				}
			}

			cache.columnName = columnName;
			cache.arr_format = new int[columnName.length];

		}catch(Exception e){
			System.out.println("Error at filter");
			e.printStackTrace();
		}

	}

	
	public static void filter(RandomAccessFile file, String[] cmp, String[] columnName, Cache cache){
		try{
			int numPages = pages(file);
			
			for(int page = 1; page <= numPages; page++){
				file.seek((page-1)*pageSize);
				byte pageType = file.readByte();
				if(pageType == 0x05)
					continue;
				else{
					byte numCells = Page.getCellNumber(file, page);

					for(int i=0; i < numCells; i++){
						long loc = Page.getCellLoc(file, page, i);
						file.seek(loc+2); 
						int rowid = file.readInt(); 
						int num_cols = new Integer(file.readByte()); 
						String[] payload = retrievePayload(file, loc);

						boolean check = cmpCheck(payload, rowid, cmp, columnName);
						if(check)
							cache.add(rowid, payload);
					}
				}
			}

			cache.columnName = columnName;
			cache.arr_format = new int[columnName.length];

		}catch(Exception e){
			System.out.println("Error at filter");
			e.printStackTrace();
		}

	}

	
	public static int pages(RandomAccessFile file){
		int num_pages = 0;
		try{
			num_pages = (int)(file.length()/(new Long(pageSize)));
		}catch(Exception e){
			System.out.println("Error at makeInteriorPage");
		}

		return num_pages;
	}

	
	public static boolean cmpCheck(String[] payload, int rowid, String[] cmp, String[] columnName){

		boolean check = false;
		if(cmp.length == 0){
			check = true;
		}else{
			int colPos = 1;
			for(int i = 0; i < columnName.length; i++){
				if(columnName[i].equals(cmp[0])){
					colPos = i + 1;
					break;
				}
			}
			String opt = cmp[1];
			String val = cmp[2];
			if(colPos == 1){
				switch(opt){
					case "=": if(rowid == Integer.parseInt(val)) 
								check = true;
							  else
							  	check = false;
							  break;
					case ">": if(rowid > Integer.parseInt(val)) 
								check = true;
							  else
							  	check = false;
							  break;
					case "<": if(rowid < Integer.parseInt(val)) 
								check = true;
							  else
							  	check = false;
							  break;
					case ">=": if(rowid >= Integer.parseInt(val)) 
								check = true;
							  else
							  	check = false;	
							  break;
					case "<=": if(rowid <= Integer.parseInt(val)) 
								check = true;
							  else
							  	check = false;	
							  break;
					case "<>": if(rowid != Integer.parseInt(val))  
								check = true;
							  else
							  	check = false;	
							  break;						  							  							  							
				}
			}else{
				if(val.equals(payload[colPos-1]))
					check = true;
				else
					check = false;
			}
		}
		return check;
	}

	public static void initializeDataStore() {


		try {
			davisbaseTablesCatalog = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
			davisbaseTablesCatalog.setLength(pageSize);
			davisbaseTablesCatalog.seek(0);
			davisbaseTablesCatalog.write(0x0D);
			davisbaseTablesCatalog.write(0x02);
			int[] offset=new int[2];
			int size1=24;
			int size2=25;
			offset[0]=pageSize-size1;
			offset[1]=offset[0]-size2;
			davisbaseTablesCatalog.writeShort(offset[1]);
			davisbaseTablesCatalog.writeInt(0);
			davisbaseTablesCatalog.writeInt(10);
			davisbaseTablesCatalog.writeShort(offset[1]);
			davisbaseTablesCatalog.writeShort(offset[0]);
			davisbaseTablesCatalog.seek(offset[0]);
			davisbaseTablesCatalog.writeShort(20);
			davisbaseTablesCatalog.writeInt(1); 
			davisbaseTablesCatalog.writeByte(1);
			davisbaseTablesCatalog.writeByte(28);
			davisbaseTablesCatalog.writeBytes("davisbase_tables");
			davisbaseTablesCatalog.seek(offset[1]);
			davisbaseTablesCatalog.writeShort(21);
			davisbaseTablesCatalog.writeInt(2); 
			davisbaseTablesCatalog.writeByte(1);
			davisbaseTablesCatalog.writeByte(29);
			davisbaseTablesCatalog.writeBytes("davisbase_columns");
		}
		catch (Exception e) {
			System.out.println("Unable to create the database_tables file");
			System.out.println(e);
		}
		try {
			davisbaseColumnsCatalog = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
			davisbaseColumnsCatalog.setLength(pageSize);
			davisbaseColumnsCatalog.seek(0);       
			davisbaseColumnsCatalog.writeByte(0x0D); 
			davisbaseColumnsCatalog.writeByte(0x08); 
			int[] offset=new int[10];
			offset[0]=pageSize-43;
			offset[1]=offset[0]-47;
			offset[2]=offset[1]-44;
			offset[3]=offset[2]-48;
			offset[4]=offset[3]-49;
			offset[5]=offset[4]-47;
			offset[6]=offset[5]-57;
			offset[7]=offset[6]-49;
			offset[8]=offset[7]-49;
			davisbaseColumnsCatalog.writeShort(offset[8]); 
			davisbaseColumnsCatalog.writeInt(0); 
			davisbaseColumnsCatalog.writeInt(0);
			
			for(int i=0;i<9;i++)
				davisbaseColumnsCatalog.writeShort(offset[i]);

			
			davisbaseColumnsCatalog.seek(offset[0]);
			davisbaseColumnsCatalog.writeShort(33); 
			davisbaseColumnsCatalog.writeInt(1); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(28);
			davisbaseColumnsCatalog.writeByte(17);
			davisbaseColumnsCatalog.writeByte(15);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
		    davisbaseColumnsCatalog.writeBytes("davisbase_tables"); 
			davisbaseColumnsCatalog.writeBytes("rowid"); 
			davisbaseColumnsCatalog.writeBytes("INT"); 
			davisbaseColumnsCatalog.writeByte(1); 
			davisbaseColumnsCatalog.writeBytes("NO"); 
			davisbaseColumnsCatalog.seek(offset[1]);
			davisbaseColumnsCatalog.writeShort(39); 
			davisbaseColumnsCatalog.writeInt(2); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(28);
			davisbaseColumnsCatalog.writeByte(22);
			davisbaseColumnsCatalog.writeByte(16);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			davisbaseColumnsCatalog.writeBytes("davisbase_tables");
			davisbaseColumnsCatalog.writeBytes("table_name");  
			davisbaseColumnsCatalog.writeBytes("TEXT");
			davisbaseColumnsCatalog.writeByte(2); 
			davisbaseColumnsCatalog.writeBytes("NO"); 
		    davisbaseColumnsCatalog.seek(offset[2]);
			davisbaseColumnsCatalog.writeShort(34); 
			davisbaseColumnsCatalog.writeInt(3); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(17);
			davisbaseColumnsCatalog.writeByte(15);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("rowid");
			davisbaseColumnsCatalog.writeBytes("INT");
			davisbaseColumnsCatalog.writeByte(1);
			davisbaseColumnsCatalog.writeBytes("NO");
		    davisbaseColumnsCatalog.seek(offset[3]);
			davisbaseColumnsCatalog.writeShort(40); 
			davisbaseColumnsCatalog.writeInt(4); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(22);
			davisbaseColumnsCatalog.writeByte(16);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("table_name");
			davisbaseColumnsCatalog.writeBytes("TEXT");
			davisbaseColumnsCatalog.writeByte(2);
			davisbaseColumnsCatalog.writeBytes("NO");
			davisbaseColumnsCatalog.seek(offset[4]);
			davisbaseColumnsCatalog.writeShort(41);
			davisbaseColumnsCatalog.writeInt(5); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(23);
			davisbaseColumnsCatalog.writeByte(16);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
	        davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("column_name");
			davisbaseColumnsCatalog.writeBytes("TEXT");
			davisbaseColumnsCatalog.writeByte(3);
			davisbaseColumnsCatalog.writeBytes("NO");
	        davisbaseColumnsCatalog.seek(offset[5]);
			davisbaseColumnsCatalog.writeShort(39); 
			davisbaseColumnsCatalog.writeInt(6); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(21);
			davisbaseColumnsCatalog.writeByte(16);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("data_type");
			davisbaseColumnsCatalog.writeBytes("TEXT");
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeBytes("NO");
			davisbaseColumnsCatalog.seek(offset[6]);
			davisbaseColumnsCatalog.writeShort(49); 
			davisbaseColumnsCatalog.writeInt(7); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(28);
			davisbaseColumnsCatalog.writeByte(19);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("ordinal_position");
			davisbaseColumnsCatalog.writeBytes("TINYINT");
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeBytes("NO");
			davisbaseColumnsCatalog.seek(offset[7]);
			davisbaseColumnsCatalog.writeShort(41);
			davisbaseColumnsCatalog.writeInt(8); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(23);
			davisbaseColumnsCatalog.writeByte(16);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("is_nullable");
			davisbaseColumnsCatalog.writeBytes("TEXT");
			davisbaseColumnsCatalog.writeByte(6);
			davisbaseColumnsCatalog.writeBytes("NO");
		}
		catch (Exception e) {
			System.out.println("Unable to create the database_columns file");
			System.out.println(e);
		}
	}
}


