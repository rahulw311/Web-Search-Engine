import java.io.IOException;
import java.io.*;
import java.nio.*;
import java.util.*;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.FSDirectory;
public class lucene_index {
    public static void main(String[] args) throws IOException, ParseException {

        //Storing Arguments to variables
        Analyzer analyzer = new StandardAnalyzer();
        String INDEX_DIRECTORY = "./lucene_directory";
        File f1= new File(INDEX_DIRECTORY);
        ////Storing index in memory //only use either memory or disk
        //Directory myDirectory = new RAMDirectory();
        //Storing index on disk //only use either memory or disk
        Directory myDirectory = FSDirectory.open(f1.toPath());

        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter indexWriter = new IndexWriter(myDirectory, config);
        Scanner sc = new Scanner(new File("C:\\Users\\rahul\\IR\\crawler_data.csv"),"UTF-8");
        sc.useDelimiter(",");   //sets the delimiter pattern
        String url=null;
        String title=null;
        String filename=null;
        String doc_id=null;
        int Count_doc=0; // get the start time
        long start = System.nanoTime();
        while (sc.hasNextLine())
        {
            String[] line = sc.nextLine().split(",");
            if(line.length != 4)
                continue;
            doc_id=line[0].toString();
            url=line[1].toString();
            filename=line[2].toString();
            if(filename.equals("content")) continue;
            title=line[3].toString();
            try{
                File file=new File("C:\\Users\\rahul\\IR\\pages\\"+filename);
                StringBuilder resultStringBuilder = new StringBuilder();
                BufferedReader br=new BufferedReader(new FileReader(file));
                String lines;
                while ((lines = br.readLine()) != null) {
                    resultStringBuilder.append(lines).append("\n");
                }
                String data= resultStringBuilder.toString();
                Document doc = new Document();
                doc.add(new TextField("doc_id",doc_id,Field.Store.NO));
                doc.add(new TextField("url",url,Field.Store.YES));
                doc.add(new TextField("content",data,Field.Store.YES));
                doc.add(new TextField("title",title,Field.Store.YES));
                indexWriter.addDocument(doc);
                Count_doc++;
                if(Count_doc%1600==0){
                    long time_end = System.nanoTime();
                    long time_print = time_end - start;
                    double print_time = (double) time_print / 1_000_000_000;
                    System.out.println("Time taken to create the index for "+Count_doc + " documents is :"  + print_time + " seconds");
                }
            }
            catch(FileNotFoundException e){
                continue;
            }
        }
        sc.close();
        indexWriter.close();
    }
}
