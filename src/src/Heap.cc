#include "DBFile.h"
#include "GenericDBFile.h"

Heap::Heap()
{}
/*				  m_sFilePath(), m_pPage(NULL), m_nTotalPages(0),
				  m_bDirtyPageExists(false), m_bIsDirtyMetadata(false),
				  m_pCurrPage(NULL), m_nCurrPage(0), 
				  m_pCurrPageCNF(NULL), m_nCurrPageCNF(0)
{
	m_pFile = new File();
}*/

Heap::~Heap()
{ }
/*{
	// delete member File pointer
	if (m_pFile)
	{
		delete m_pFile;
		m_pFile = NULL;
	}

	// delete member Page pointer
	if (m_pPage)
	{
		delete m_pPage;
		m_pPage = NULL;
	}
}*/

int Heap::Create(char *f_path)
{
	GenericDBFile::Create(f_path, NULL);
}

int Heap::Open(char *fname)
{
	EventLogger *el = EventLogger::getEventLogger();

	// check if file exists
	struct stat fileStat; 
  	int iStatus; 

	iStatus = stat(fname, &fileStat); 
	if (iStatus != 0) 	// file doesn't exists
	{ 
        el->writeLog("File " + string(fname) + " does not exist.\n");
        return RET_FILE_NOT_FOUND;
	} 

	// TODO for multithreaded env: 
	//		check if file is NOT open already - why ?
	// 		ans - coz we should not access an already open file
	// 		eg - currently open by another thread (?)

	/* ------- Not Used Currently -----
	// Read m_nTotalPages from metadata file, if meta.data file exists
	string meta_file_name = m_sFilePath + ".meta.data";
	iStatus = stat(meta_file_name.c_str(), &fileStat);
	if (iStatus == 0)
	{
		ifstream meta_in;
		meta_in.open(meta_file_name.c_str(), ifstream::in);
		meta_in >> m_nTotalPages;
		meta_in.close();
	}*/

	// open file in append mode, preserving all prev content
	if (m_pFile)
	{
		m_pFile->Open(APPEND, const_cast<char*>(fname));//openInAppendMode
                m_nTotalPages = m_pFile->GetLength() - 2;   //get total number of pages which are in the file

                if(!m_pPage)
                    m_pPage = new Page();
                if(m_nTotalPages >= 0)
                    m_pFile->GetPage(m_pPage, m_nTotalPages);   //fetch last page from DB
                else
                    m_nTotalPages = 0;
	}
	
	return RET_SUCCESS;
}

// returns 1 if successfully closed the file, 0 otherwise 
int Heap::Close()
{
	GenericDBFile::Close();
	WriteMetaData(); 

	return 1; // If control came here, return success
}

/* Load function bulk loads the Heap instance from a text file, appending
 * new data to it using the SuckNextRecord function from Record.h. The character
 * string passed to Load is the name of the data file to bulk load.
 */
void Heap::Load (Schema &mySchema, char *loadMe)
{
	EventLogger *el = EventLogger::getEventLogger();

	FILE *fileToLoad = fopen(loadMe, "r");
	if (!fileToLoad)
	{
		el->writeLog("Can't open file name :" + string(loadMe));
	}

	//open the dbfile instance
	Open(const_cast<char*>(m_sFilePath.c_str()));

	/* Logic : 
     * first read the record from the file using suckNextRecord()
	 * then add this record to page using Add() function
	 * Write dirty data to file before leaving this function
	 */

	Record aRecord;
	while(aRecord.SuckNextRecord(&mySchema, fileToLoad))
		Add(aRecord);

	WritePageToFile();
}

void Heap::Add (Record &rec, bool startFromNewPage)
{
	EventLogger *el = EventLogger::getEventLogger();
	
	// Consume the record
	Record aRecord;
	aRecord.Consume(&rec);

	/* Logic: 
	 * Try adding the record to the current page
	 * if adding fails, write page to file and create new page
     * mark m_bDirtyPageExists = true after adding record to page
	 */

	// Writing data in the file for the first time
	if (m_pPage == NULL)
	{
		m_pPage = new Page();
		m_nTotalPages = 0;
	}

        if(startFromNewPage)
        {
            WritePageToFile();  //this will write only if dirty page exists
            m_pPage->EmptyItOut();
            m_nTotalPages++;
        }
        // a page exists in memory, add record to it
        if (m_pPage)
        {
            if (!m_pPage->Append(&aRecord)) // current page does not have enough space
            {
                // write current page to file
                // this function will fetch a new page too
                WritePageToFile();
                if (!m_pPage->Append(&aRecord))
                {
                                el->writeLog("Heap::Add --> Adding record to page failed.\n");
                                return;
                }
                else
                    m_bDirtyPageExists = true;
            }
            else
                m_bDirtyPageExists = true;
        }   //else part would never occur so we can remove this IF condition
}

void Heap::MoveFirst ()
{
	GenericDBFile::MoveFirst();
}

// Function to fetch the next record in the file in "fetchme"
// Returns 0 on failure
int Heap::GetNext (Record &fetchme)
{
	return GenericDBFile::GetNext(fetchme);
}


// Function to fetch the next record in "fetchme" that matches 
// the given CNF, returns 0 on failure.
int Heap::GetNext (Record &fetchme, CNF &cnf, Record &literal)
{
	/* logic :
	 * first read the record from the file in "fetchme,
	 * pass it for comparison using given cnf and literal.
	 * if compEngine.compare returns 1, it means fetched record
	 * satisfies CNF expression so we simple return success (=1) here
	 */

	ComparisonEngine compEngine;

	while (GetNext(fetchme))
	{
		if (compEngine.Compare(&fetchme, &literal, &cnf))
			return RET_SUCCESS;
	}

	//if control is here then no matching record was found
	return RET_FAILURE;
}


// Write dirty page to file - Needed now??
void Heap::WritePageToFile()
{
    if (m_bDirtyPageExists)
    {
        m_pFile->AddPage(m_pPage, m_nTotalPages++);
        m_pPage->EmptyItOut();
        // everytime page count increases, set m_bIsDirtyMetadata to true
        m_bIsDirtyMetadata = true;
    }
    m_bDirtyPageExists = false;
}

// Create <table_name>.meta.data file
// And write total pages used for table loading in it
void Heap::WriteMetaData()
{
   if (m_bIsDirtyMetadata && !m_sFilePath.empty())
   {
		ofstream meta_out;
		meta_out.open(string(m_sFilePath + ".meta.data").c_str(), ios::trunc);
		meta_out << "heap";
		meta_out.close();
		m_bIsDirtyMetadata = false;
   }
}

