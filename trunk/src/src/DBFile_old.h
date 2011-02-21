#ifndef DB_FILE_H
#define DB_FILE_H


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
#include "EventLogger.h"

// Enum for file types
typedef enum
{
	heap,
	sorted,
	tree
} fType;

// Enum for file opening modes
enum FileOpenMode { TRUNCATE = 0, APPEND = 1};

class DBFile
{
	private:
		string m_sFilePath;	// path of the .bin file
		File *m_pFile;		// .bin file where data will be loaded
		Page *m_pPage;
		int m_nTotalPages;
		bool m_bDirtyPageExists;
		bool m_bIsDirtyMetadata;
		// Member variables to handle GetNext()
		Page *m_pCurrPage;
		int  m_nCurrPage;
		// Member variables to handle GetNext(CNF)
		Page *m_pCurrPageCNF;
		int  m_nCurrPageCNF;

		// Private member functions
		void WritePageToFile();
		int FetchNextRec(Record &fetchme, Page**, int&);

		// This method is currently unused. But will be used later
		//void WriteMetaData();

	public:
		DBFile();
		~DBFile();

		// name = location of the file
		// fType = heap, sorted, tree
		// return value: 1 on success, 0 on failure
		int Create (char *name, fType myType, void *startup); 

		// This function assumes that the DBFile already exists
		// and has previously been created and then closed.
		int Open (char *name);

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
		void Add (Record &addMe, bool startFromNewPage = false);

		// Fetch next record (relative to p_currPtr) into fetchMe
		int GetNext (Record &fetchMe);

		// Applies CNF and then fetches the next record
		int GetNext (Record &fetchMe, CNF &applyMe, Record &literal);

		// Used in assignment-2
		void GetPage(Page *putItHere, off_t whichPage);
		int TotalPages()
		{
			return m_pFile->GetLength();
		}
};

#endif
