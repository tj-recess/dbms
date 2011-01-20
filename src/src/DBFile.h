#ifndef DB_FILE_H
#define DB_FILE_H


#include "Record.h"
#include "File.h"
#include <string.h>
#include <iostream>
#include <stdlib.h>
#include "Defs.h"
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "ErrorLogger.h"

// Enum for file types
typedef enum
{
	heap,
	sorted,
	tree
} fType;

const int openInAppendMode = 1;
const int openInTruncateMode = 0;

class DBFile
{
	private:
		string m_sFilePath;
		File *m_pFile;		// file where data will be loaded
		Page *m_pPage;
		int m_nTotalPages;
		void WritePageToFile();

	public:
		DBFile();
		~DBFile();

		// name = location of the file
		// fType = heap, sorted, tree
		// return value: 1 on success, 0 on failure
		int Create (char *name, fType myType, void *startup); 

		// This function assumes that the DBFile already exists
		// and has previously been created and then closed.
		int Open (const char *name);

		// Closes the file. 
		// The return value is a 1 on success and a zero on failure
		int Close (); 

		// Bulk loads the DBFile instance from a text file, 
		// appending new data to it using the SuckNextRecord function from Record.h
		// loadMe is the name of the data file to bulk load. 
		void Load (Schema &mySchema, char *loadMe);

		// Forces the pointer to correspond to the first record in the file
		void MoveFirst();

		// Add new record to the end of the file
		// Note: addMe is consumed by this function and cannot be used again
		void Add (Record &addMe);

		// Fetch next record (relative to p_currPtr) into fetchMe
		int GetNext (Record &fetchMe);

		// Applies CNF and then fetches the next record
		int GetNext (Record &fetchMe, CNF &applyMe, Record &literal);

};

#endif

/*
 * #ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

typedef enum {heap, sorted, tree} fType;

// stub DBFile header..replace it with your own DBFile.h

class DBFile {

public:
	DBFile ();

	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();

	void Load (Schema &myschema, char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif
 */

