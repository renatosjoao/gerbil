package org.aksw.gerbil.semantic.sameas.index;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

public class Searcher extends LuceneConstants{

	   IndexSearcher indexSearcher;
	   QueryParser queryParser;
	   Query query;

	   public Searcher(String indexDirectoryPath) throws IOException{
	      Directory indexDirectory = 
	         FSDirectory.open(new File(indexDirectoryPath));
	      indexSearcher = new IndexSearcher(indexDirectory);
	      queryParser = new QueryParser(CONTENTS,
	         new StandardAnalyzer(Version.LUCENE_CURRENT));
	   }

	   public TopDocs search( String searchQuery) 
	      throws IOException, ParseException{
	      query = queryParser.parse(searchQuery);
	      return indexSearcher.search(query, MAX_SEARCH);
	   }

	   public Document getDocument(ScoreDoc scoreDoc) 
	      throws CorruptIndexException, IOException{
	     return indexSearcher.doc(scoreDoc.doc);	
	   }

	   public void close() throws IOException{
	      indexSearcher.close();
	   }
	
	   public List<String> searchSameAs(String uri) throws IOException, ParseException{
		   TopDocs docs = search(uri);
		   List<String> uris = new LinkedList<String>();
		   for(ScoreDoc scoreDoc : docs.scoreDocs){
			   Document doc = getDocument(scoreDoc);		 
			   File f = new File(doc.get(FILE_PATH));
			   BufferedReader reader = new BufferedReader(new FileReader(f));
			   String line="";
			   while((line=reader.readLine())!=null){
				   if(line.isEmpty())
					   continue;
				   uris.add(line);
			   }
		   }		   
		   return uris;
	   }
	   
}
