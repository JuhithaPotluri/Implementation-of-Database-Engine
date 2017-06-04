

import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.SortedMap;


public class DavisBase {

	
	static String prompt = "davisbase:> ";
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");
public static void main(String[] args) {
    	init();
		welcomeScreen();
String userCommand = ""; 
       while(!userCommand.equals("exit")) {
			System.out.print(prompt);
			userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
			parseUserCommand(userCommand);
		}
		System.out.println("Exited");
		
}

	public static void welcomeScreen() {
	
        System.out.println("Welcome to DavisBase"); 
        version();
        System.out.println("@:Juhitha Potluri");
		System.out.println("Type \" help;\" to display list of supported commands.\n");
		System.out.println(line("*",80));
	}
    private static void writeData(){

        FileOutputStream fos = null;

        try {

            fos = new FileOutputStream("data.dat");

        } catch (FileNotFoundException e) {

            e.printStackTrace();

        }

        try {

            ObjectOutputStream oos = new ObjectOutputStream(fos);

            oos.writeObject(DelUse.delMap);

        } catch (IOException e) {

            e.printStackTrace();

        }

        
        
        
        
    }
	
public static String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}
	
	
	public static void help() {
		
		System.out.println("SUPPORTED COMMANDS:");
		System.out.println("(Commands are case insensitive)");
		System.out.println();
		System.out.println("1)CREATE TABLE table_name (col1 datatype , col2 datatype,...);  Creates a table(first column should be an integer(primary key)).");
		System.out.println("2)INSERT into table_name ( col1 , col2.. ) values ( val1 , val2.... );  Inserts records into the table.");
		System.out.println("3)UPDATE table_name SET col_name=value WHERE col_name1=value1;  Update the specified record.");
		System.out.println("4)SELECT * FROM table_name WHERE rowid = <value>;  Display all records whose rowid=<value>.");
		System.out.println("5)SELECT * FROM table_name; Display all records in the table.");
		System.out.println("6)SHOW TABLES;  Display all tables in the database.");
		System.out.println("7)DROP TABLE table_name;  Remove table data and its schema.");
		System.out.println("8)DELETE FROM table_name WHERE some_column=some_value;  Deletes the stated record.");
		System.out.println("9)HELP;  Show help information");
		System.out.println("10)EXIT;  Exit the program");
		System.out.println();	
	}

	
public static void version() {
		System.out.println("DavisBase v1.0");
	}

	
	public static boolean tableExist(String table){
		boolean e = false;
		table = table+".tbl";
		try {
			File dataDir = null;
			
			
			if(table.equals("davisbase_tables.tbl") || table.equals("davisbase_columns.tbl")){
				dataDir=new File("data/catalog/");
			}
			else
			{
				dataDir=new File("data/user_data/");
			}
			
			
			String[] oldTableFiles;
			oldTableFiles = dataDir.list();
			for (int i=0; i<oldTableFiles.length; i++) {
				if(oldTableFiles[i].equals(table))
					return true;
			}
		}
		catch (SecurityException se) {
			System.out.println("Unable to create data container directory");
			System.out.println(se);
		}

		return e;
	}

	public static void init(){
		try {
			File dataDir = new File("data/user_data/");
			if(!dataDir.exists()){
				dataDir.mkdirs();
		
			}
		}catch(SecurityException se) {
				System.out.println("Unable to create data container directory");
				System.out.println(se);
			}
		
			try {
				File dataDir = new File("data/catalog/");
				if(!dataDir.exists()){
					dataDir.mkdirs();
					Table.initializeDataStore();
				}else {
				String meta1 = "davisbase_columns.tbl";
				String meta2 = "davisbase_tables.tbl";
				String[] oldTableFiles = dataDir.list();
				boolean check = false;
				for (int i=0; i<oldTableFiles.length; i++) {
					if(oldTableFiles[i].equals(meta1))
						check = true;
				}
				if(!check){
					
					Table.initializeDataStore();
				}
				check = false;
				for (int i=0; i<oldTableFiles.length; i++) {
					if(oldTableFiles[i].equals(meta2))
						check = true;
				}
				if(!check){
					
					Table.initializeDataStore();
				}
			}
		}catch (SecurityException se) {
			System.out.println("Unable to create data container directory");
			System.out.println(se);
		}

	}



	public static String[] parserEquation(String equ){
		String cmp[] = new String[3];
		String temp[] = new String[2];
		if(equ.contains("=")) {
			temp = equ.split("=");
			cmp[0] = temp[0].trim();
			cmp[1] = "=";
			cmp[2] = temp[1].trim();
		}

		if(equ.contains(">")) {
			temp = equ.split(">");
			cmp[0] = temp[0].trim();
			cmp[1] = ">";
			cmp[2] = temp[1].trim();
		}

		if(equ.contains("<")) {
			temp = equ.split("<");
			cmp[0] = temp[0].trim();
			cmp[1] = "<";
			cmp[2] = temp[1].trim();
		}

		if(equ.contains(">=")) {
			temp = equ.split(">=");
			cmp[0] = temp[0].trim();
			cmp[1] = ">=";
			cmp[2] = temp[1].trim();
		}

		if(equ.contains("<=")) {
			temp = equ.split("<=");
			cmp[0] = temp[0].trim();
			cmp[1] = "<=";
			cmp[2] = temp[1].trim();
		}

		if(equ.contains("<>")) {
			temp = equ.split("<>");
			cmp[0] = temp[0].trim();
			cmp[1] = "<>";
			cmp[2] = temp[1].trim();
		}

		return cmp;
	}
	
	
	public static void parseUserCommand (String userCommand) {
	String[] commandTokens = userCommand.split(" ");

		switch (commandTokens[0]) {
			case "init":
				Table.initializeDataStore();
				break;

			case "create":
				String create_table = commandTokens[2];
				String[] create_temp = userCommand.split(create_table);
				String col_temp = create_temp[1].trim();
				String[] create_cols = col_temp.substring(1, col_temp.length()-1).split(",");
				for(int i = 0; i < create_cols.length; i++)
					create_cols[i] = create_cols[i].trim();
				if(tableExist(create_table)){
					System.out.println("Table "+create_table+" already exists.");
					System.out.println();
					break;
				}
				Table.createTable(create_table, create_cols);

				break;

			case "drop":
				String tb = commandTokens[2];
				if(!tableExist(tb)){
					System.out.println("Table "+tb+" does not exist.");
					System.out.println();
					break;
				}
				Table.drop(tb);
				break;

			case "show":
				Table.show();
				break;

			case "insert":
				String insert_table = commandTokens[2];
				String insert_vals = userCommand.split("values")[1].trim();
				insert_vals = insert_vals.substring(1, insert_vals.length()-1);
				String[] insert_values = insert_vals.split(",");
				for(int i = 0; i < insert_values.length; i++)
					insert_values[i] = insert_values[i].trim();
				if(!tableExist(insert_table)){
					System.out.println("Table "+insert_table+" does not exist.");
					System.out.println();
					break;
				}
				Table.insertInto(insert_table, insert_values);
				break;
			case "delete":
				 String[] delete = userCommand.split("where");



                String[] table = delete[0].trim().split("from");



                String[] table1 = table[1].trim().split(" ");



                String tableName = table1[0].trim();

                System.out.println("delete:" + tableName);



                String[] terms = delete[1].split("=");

                int id = Integer.parseInt(terms[1].trim());



                if (!tableExist(tableName)) {

                    System.out.println("Table " + tableName + " does not exist.");

                    System.out.println();

                    break;

                }

                tableName = tableName.toLowerCase().trim();

                if(DelUse.delMap.get(tableName) == null){

                    DelUse.delMap.put(tableName, new HashSet<>());

                }

                Set<Integer> temp_set = DelUse.delMap.get(tableName);

                temp_set.add(id);
                DelUse.delMap.put(tableName, temp_set);
                writeData();
                break;
                
                

			case "update":
				String update_table = commandTokens[1];
				String[] update_temp1 = userCommand.split("set");
				String[] update_temp2 = update_temp1[1].split("where");
				String update_cmp_s = update_temp2[1];
				String update_set_s = update_temp2[0];
				String[] set = parserEquation(update_set_s);
				String[] update_cmp = parserEquation(update_cmp_s);
				if(!tableExist(update_table)){
					System.out.println("Table "+update_table+" does not exist.");
					System.out.println();
					break;
				}
				Table.update(update_table, set, update_cmp);
				break;
				
			case "select":
				String[] select_cmp;
				String[] select_column;
				String[] select_temp = userCommand.split("where");
				if(select_temp.length > 1){
					String filter = select_temp[1].trim();
					select_cmp = parserEquation(filter);
				}else{
					select_cmp = new String[0];
				}
				String[] select = select_temp[0].split("from");
				String select_table = select[1].trim();
				String select_cols = select[0].replace("select", "").trim();
				if(select_cols.contains("*")){
					select_column = new String[1];
					select_column[0] = "*";
				}
				else{
					select_column = select_cols.split(",");
					for(int i = 0; i < select_column.length; i++)
						select_column[i] = select_column[i].trim();
				}
				if(!tableExist(select_table)){
					System.out.println("Table "+select_table+" does not exist.");
					System.out.println();
					break;
				}
				Table.select(select_table, select_column, select_cmp);
				break;

			case "help":
				help();
				break;

			case "version":
				version();
				break;

			case "exit":
				break;

			default:
				System.out.println("Please use a valid command:\"" + userCommand + "\"");
				System.out.println();
				break;
		}
	} 
	
}