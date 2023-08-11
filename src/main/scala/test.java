
import com.opencsv.CSVReader;
import java.io.FileReader;
import java.util.Arrays;

public class test {
    public static void main(String[] args) {
        try (CSVReader reader = new CSVReader(new FileReader("D:\\工作\\2023\\3月\\省立医院数抽样\\随访_诊断_处方_病历\\sample_note.rds"))) {
            String[] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                String noteText = nextLine[5];
                String[] noteTextSplit = noteText.split("\"\"");
                System.out.println(noteTextSplit[1].toString());
//                System.out.println(noteTextSplit);
//                String[] noteTextColumns = Arrays.copyOfRange(noteTextSplit, 1, noteTextSplit.length - 1);
                // Do something with the noteTextColumns array
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

