package org.aksw.gerbil.semantic.sameas.index;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import org.aksw.gerbil.datatypes.ErrorTypes;
import org.aksw.gerbil.exceptions.GerbilException;
import org.aksw.gerbil.semantic.sameas.impl.http.HTTPBasedSameAsRetriever;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Indexer extends LuceneConstants{

	private static final Logger LOGGER = LoggerFactory
			.getLogger(HTTPBasedSameAsRetriever.class);

	

	private IndexWriter writer;

	public Indexer(String path) throws GerbilException {
		try {
			Directory dir = FSDirectory.open(new File(path));
			writer = new IndexWriter(dir, new StandardAnalyzer(
					Version.LUCENE_CURRENT), true,
					IndexWriter.MaxFieldLength.UNLIMITED);
		} catch (IOException e) {
			LOGGER.error("Error occured during accesing file " + path, e);
			throw new GerbilException(ErrorTypes.UNEXPECTED_EXCEPTION);
		}
	}

	public void close() {
		try {
			writer.close();
		} catch (IOException e) {
			LOGGER.error("Error occured during closing Index Writer", e);
		}
	}

	public void indexDocument(File f) {
		Document doc = convertFile(f);
		if (doc == null)
			return;
		try {
			writer.addDocument(doc);
		} catch (IOException e) {
			LOGGER.info("Could not index file " + f + " due to IO Exception", e);
		}
	}
	
	public void indexDocument(String content){
		indexDocument(new StringReader(content));
	}
	
	public void indexDocument(Reader reader){
		Document doc = convertStream(reader);
		if (doc == null)
			return;
		try {
			writer.addDocument(doc);
		} catch (IOException e) {
			LOGGER.info("Could not index due to IO Exception", e);
		}
	}

	private Document convertStream(Reader reader) {
		Document document = new Document();
		Field contentField = new Field(CONTENTS, reader);
		document.add(contentField);

		return document;
	}

	private Document convertFile(File f) {
		Document document = new Document();
		try {
			Field contentField = new Field(CONTENTS, new FileReader(f));
			// index file name
			Field fileNameField = new Field(FILE_NAME, f.getName(),
					Field.Store.YES, Field.Index.NOT_ANALYZED);
			// index file path
			Field filePathField = new Field(FILE_PATH, f.getCanonicalPath(),
					Field.Store.YES, Field.Index.NOT_ANALYZED);
			document.add(contentField);
			document.add(fileNameField);
			document.add(filePathField);

		} catch (IOException e) {
			LOGGER.info("Could not index File" + f.getName()
					+ " due to IO Errors", e);
			return null;
		}

		return document;
	}

}
