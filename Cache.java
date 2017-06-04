import java.util.HashMap;
import java.util.Set;

public class Cache{
	public int nRows;  
	public HashMap<Integer, String[]> content;
	public int[] arr_format; 
	public String[] columnName; 


public Cache(){
		nRows = 0;
		content = new HashMap<Integer, String[]>();
	}

public void add(int rowId, String[] value){
		content.put(rowId, value);
		nRows = nRows + 1;
	}

	public void updatearr_Format(){
		for(int i = 0; i < arr_format.length; i++)
			arr_format[i] = columnName[i].length();
		for(String[] i : content.values()){
			for(int j = 0; j < i.length; j++)
				if(arr_format[j] < i[j].length())
					arr_format[j] = i[j].length();
		}
	}

	
	public String fix(int length, String s) {
		return String.format("%-"+(length+3)+"s", s);
	}

	
	public String line(String s,int length) {
		String a = "";
		for(int i=0;i<length;i++) {
			a += s;
		}
		return a;
	}

	
public void display(String[] col, String tableName){
		
Set<Integer> numrows = null;
if (DelUse.delMap.get(tableName) != null) {
   numrows = DelUse.delMap.get(tableName);
}
if (nRows == 0) {
System.out.println("Empty set.");
}
else {
updatearr_Format();

 if (col[0].equals("*")) {
            for (int l : arr_format)
            System.out.print(line("-", l + 3));
            System.out.println();
            for (int j = 0; j < columnName.length; j++)
            System.out.print(fix(arr_format[j], columnName[j]) + "|");
            System.out.println();
for (int l : arr_format)
System.out.print(line("-", l + 3));
System.out.println();
boolean flag = false;
for (String[] i : content.values()) {
flag = false;
for (int j = 0; j < i.length; j++) {
      if (numrows!= null) {
        if (numrows.contains(Integer.parseInt(i[0].trim()))) {
                 flag = true;
                 break;
             }
        }

System.out.print(fix(arr_format[j], i[j]) + "|");

            }
 if(!flag)

      System.out.println();
}
System.out.println();
} 
 else {
int[] control = new int[col.length];

 for (int j = 0; j < col.length; j++)

    for (int i = 0; i < columnName.length; i++)
              if (col[j].equals(columnName[i]))

                    control[j] = i;
   for (int j = 0; j < control.length; j++)

            System.out.print(line("-", arr_format[control[j]] + 3));
            System.out.println();

   for (int j = 0; j < control.length; j++)

            System.out.print(fix(arr_format[control[j]], columnName[control[j]]) + "|");
            System.out.println();
   for (int j = 0; j < control.length; j++)
           System.out.print(line("-", arr_format[control[j]] + 3));
           System.out.println();
           boolean flag = false;
   for (String[] i : content.values()) {
     flag = false;
    for (int j = 0; j < control.length; j++) {
           if (numrows != null) {
                if (numrows.contains(Integer.parseInt(i[0].trim()))) {
                       flag = true;
                       break;
                   }
                 }

    System.out.print(fix(arr_format[control[j]], i[control[j]]) + "|");

     }

 System.out.println();
 }

 System.out.println();

        }

     }

   }
}