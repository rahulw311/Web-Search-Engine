import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import py4j.GatewayServer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.round;

class index_searcher{
    public List<List<String>> LuceneSearch(String queryInput) throws IOException, ParseException {

        List<List<String>> resultList = new ArrayList<List<String>>();

        String queryName = queryInput;
        String INDEX_DIRECTORY = "./lucene_directory";
        File f1= new File(INDEX_DIRECTORY);
        //Defining the directory for the lucene index
        Directory myDirectory = FSDirectory.open(f1.toPath());

        Analyzer analyzer =  new StandardAnalyzer();

        DirectoryReader indexReader = DirectoryReader.open(myDirectory);
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);
        QueryParser parser = new QueryParser("content",analyzer);
        Query query = parser.parse(queryName);

        //opening a new file
        String Result = "./result.txt";
        File r= new File(Result);
        FileWriter fw = new FileWriter(r.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);

        long search_start = System.nanoTime();
        int topHitCount =10;
        ScoreDoc[] hits =indexSearcher.search(query,topHitCount).scoreDocs;
        if (hits.length == 0)
        {
            System.out.println("Found Nothing, try some other search.");
        }
        // Search end , get the end time .
        long search_end = System.nanoTime();
        // searching  time
        long search_execution = search_end - search_start;
        double TimeInSecond = (double) search_execution / 1_000_000_000;
        //    System.out.println("Time taken to create the index : " + TimeInSecond + " seconds");
        //Iterate through results
        for ( int rank=0; rank<hits.length;++rank){
            Document hitDoc =indexSearcher.doc(hits[rank].doc);
            // Write in file
            /*String content=(rank+1)+","+hits[rank].score +","+hitDoc.get("content") +"," +hitDoc.get("url");
            bw.write(content+"\n");
            */
            List<String> resultPiece=new ArrayList<String>(4);
            resultPiece.add(""+(rank+1));
            resultPiece.add(""+hits[rank].score);
            resultPiece.add(hitDoc.get("content"));
            resultPiece.add(hitDoc.get("url"));
            resultPiece.add(hitDoc.get("title"));
            //System.out.println(resultPiece);
            resultList.add(resultPiece);
//System.out.println((rank+1)+"(Score of this Document : "+ hits[rank].score +") ---->" +hitDoc.get("content") +" URL for this page is" +hitDoc.get("url"));

            //System.out.println(indexSearcher.explain(query, hits[rank].doc));
        }
        String add="Search Completed to fetch the top 10 results";
        bw.write(add+"\n");
        String comment="Time taken to fetch the top " +hits.length +" results : " + TimeInSecond + " seconds";
        bw.write(comment);
//System.out.println("Search Completed to fetch the top 10 results");
        //System.out.println("Time taken to fetch the top " +hits.length +" results : " + TimeInSecond + " seconds");
        bw.close();
        indexReader.close();
        myDirectory.close();
        System.out.println(resultList);
        return resultList;
    }

    public static void main(String[] args) throws IOException, ParseException {

        index_searcher app = new index_searcher();
        // app is now the gateway.entry_point
        GatewayServer server = new GatewayServer(app);
        System.out.println("Starting the server..");
        server.start();
        System.out.println("Server Started");
    }
}