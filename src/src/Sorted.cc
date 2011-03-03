#include "Sorted.h"

Sorted::Sorted() : m_pSortInfo(NULL), m_bReadingMode(true), m_pBigQ(NULL), m_sMetaSuffix(".meta.data")
{
	m_pFile = new FileUtil();
	m_pINPipe = new Pipe(PIPE_SIZE);
	m_pOUTPipe = new Pipe(PIPE_SIZE);
}

Sorted::~Sorted()
{
	delete m_pFile;
    m_pFile = NULL;

	delete m_pBigQ;
	m_pBigQ = NULL;
}

int Sorted::Create(char *f_path, void *sortInfo)
{
    //ignore parameter sortInfo - not required for this file type
    m_pFile->Create(f_path);

	if (sortInfo == NULL)
	{
		cout << "\nSorted::Create --> sortInfo is NULL. ERROR!\n";
		return RET_FAILURE;
	}
	m_pSortInfo = (SortInfo*)sortInfo; 	// TODO: or make a deep copy?
	WriteMetaData();
        m_pSortInfo->myOrder->Print();
	return RET_SUCCESS;
}

int Sorted::Open(char *fname)
{
    //read metadata here
    ifstream meta_in;
    cout<<fname;
    meta_in.open((string(fname) + m_sMetaSuffix).c_str());
    string fileType;
    string type;
    m_pSortInfo = new SortInfo();
    m_pSortInfo->myOrder = new OrderMaker();
    meta_in >> fileType;
    meta_in >> m_pSortInfo->myOrder->numAtts;
    for (int i = 0; i < m_pSortInfo->myOrder->numAtts; i++)
    {
        meta_in >> m_pSortInfo->myOrder->whichAtts[i];
        meta_in >> type;
        if(type.compare("Int") == 0)
            m_pSortInfo->myOrder->whichTypes[i] = Int;
        else if(type.compare("Double") == 0)
            m_pSortInfo->myOrder->whichTypes[i] = Double;
        else
            m_pSortInfo->myOrder->whichTypes[i] = String;
    }
    return m_pFile->Open(fname);
}

// returns 1 if successfully closed the file, 0 otherwise 
int Sorted::Close()
{
	MergeBigQToSortedFile();
    return m_pFile->Close();
}

/* Load function bulk loads the Sorted instance from a text file, appending
 * new data to it using the SuckNextRecord function from Record.h. The character
 * string passed to Load is the name of the data file to bulk load.
 */
void Sorted::Load (Schema &mySchema, char *loadMe)
{
    EventLogger *el = EventLogger::getEventLogger();

    FILE *fileToLoad = fopen(loadMe, "r");
    if (!fileToLoad)
    {
		el->writeLog("Can't open file name :" + string(loadMe));
		return;	
    }

    /* Logic :
     * first read the record from the file using suckNextRecord()
     * then add this record to page using Add() function
     */

    Record aRecord;
    while(aRecord.SuckNextRecord(&mySchema, fileToLoad))
        Add(aRecord);

    fclose(fileToLoad);	

	MergeBigQToSortedFile();
}

void Sorted::Add (Record &rec)
{
	// if mode reading, change mode to writing
	if (m_bReadingMode)
		m_bReadingMode = false;

	// push rec to IN-pipe
	m_pINPipe->Insert(&rec);

	// if !BigQ, instantiate BigQ(IN-pipe, OUT-pipe, ordermaker, runlen)
	if (!m_pBigQ)
		m_pBigQ = new BigQ(*m_pINPipe, *m_pOUTPipe, *(m_pSortInfo->myOrder), m_pSortInfo->runLength);
}

void Sorted::MergeBigQToSortedFile()
{
	// Check if there's anything to merge
	if (!m_pBigQ)
		return;

	// shutdown IN pipe
	m_pINPipe->ShutDown();

	// fetch sorted records from BigQ and old-sorted-file
	// and do a 2-way merge and write into new-file (tmpFile)
	Record * pRecFromPipe = NULL, *pRecFromFile = NULL;
	ComparisonEngine ce;
	FileUtil tmpFile;

        string tmpFileName = "tmpFile" + getusec();    //time(NULL) returns time_t in seconds since 1970
	tmpFile.Create(const_cast<char*>(tmpFileName.c_str()));
//	tmpFile.Open("tmpFile");    //not required now as we don't close file in create

	m_pFile->MoveFirst();
	int fetchedFromPipe = 1, fetchedFromFile = 1;

        //if file on disk is empty (initially it will be) then don't fetch anything
        if(m_pFile->GetFileLength() == 0)
            fetchedFromFile = 0;

	while (fetchedFromPipe && fetchedFromFile)
	{
		if (pRecFromPipe == NULL)
		{
			pRecFromPipe = new Record;
			fetchedFromPipe = m_pOUTPipe->Remove(pRecFromPipe);
		}
		if (pRecFromFile == NULL)
		{
			pRecFromFile = new Record;
			fetchedFromFile = m_pFile->GetNext(*pRecFromFile);
		}

		if (fetchedFromPipe && fetchedFromFile)
		{
			if (ce.Compare(pRecFromPipe, pRecFromFile, m_pSortInfo->myOrder) < 0)
			{
				tmpFile.Add(*pRecFromPipe);
				delete pRecFromPipe;
				pRecFromPipe = NULL;
			}
			else
			{
				tmpFile.Add(*pRecFromFile);
				delete pRecFromFile;
				pRecFromFile = NULL;
			}
		}
	}

	Record rec;
	while (fetchedFromPipe && m_pOUTPipe->Remove(&rec))
	{
		tmpFile.Add(rec);
	}
	while (fetchedFromFile && m_pFile->GetNext(rec))
	{
		tmpFile.Add(rec);
        }

	tmpFile.Close();

	// if tmpFile is not empty, then delete old file
	// and rename tmpFile to old file's name
	if (tmpFile.GetFileLength() != 0)
	{
            // delete old file
            if(remove(m_pFile->GetBinFilePath().c_str()) != 0)
                perror("error in removing old file");   //remove this as file might not exist initially
            
//		string command = "rm \"" + m_pFile->GetBinFilePath() + "\"";
//		system(command.c_str());

            // rename tmp file to original old name
            if(rename(tmpFileName.c_str(), m_pFile->GetBinFilePath().c_str()) != 0)
                perror("error in renaming temp file");
//		command = "mv tmpFile \"" + m_pFile->GetBinFilePath() + "\"";
//		system(command.c_str());
	}

	// delete BigQ
	delete m_pBigQ;
	m_pBigQ = NULL;
}

void Sorted::MoveFirst ()
{
	m_pFile->MoveFirst();
}

// Function to fetch the next record in the file in "fetchme"
// Returns 0 on failure
int Sorted::GetNext (Record &fetchme)
{
	// If mode is not reading i.e. it is writing mode currently
	// merge differential data to already sorted file
	if (!m_bReadingMode)
	{
		m_bReadingMode = true;
		MergeBigQToSortedFile();
	}

	// Now we can start reading
	return m_pFile->GetNext(fetchme);
}


// Function to fetch the next record in "fetchme" that matches 
// the given CNF, returns 0 on failure.
int Sorted::GetNext (Record &fetchme, CNF &cnf, Record &literal)
{
    /*Logic:
     *
     * Logic:
     * Prepare “query” OrderMaker from applyMe (CNF) -
     * If the attribute used in Sorted file’s order maker is also present in CNF, append to “query” orderMaker
     * else - stop making “query” orderMaker
     * find the first matching record -
     * If query OrderMaker is empty, first record (from current pointer or from the beginning) is the matching record.
     * if query OrderMaker is not empty, perform binary search on file (from the current pointer) to find out a record -
     * which is equal to the literal (Record) using “query” OrderMaker and CNF (probably using only “query” OrderMaker)
     * returning apropriate value -
     * if no matching record found, return 0
     * if there is a possible matching record - start scanning the file matching every record found one by one.
     * First evaluate the record on “query” OrderMaker, then evaluate the CNF instance.
     * if the query OrderMaker doesn’t accept the record, return 0
     * if query OrderMaker does accept it, match it with CNF
     * if CNF accepts, return the record.
     * if CNF doesn’t accept, move on to next record.
     * repeat 1-2 until EOF.
     * if it’s EOF, return 0.
     * Keep the value of “query” OrderMaker and current pointer safe until user performs “MoveFirst” or some write operation.
     */
    
    OrderMaker query;
    cnf.GetSortOrders(*(m_pSortInfo->myOrder), query);
    
#ifdef _DEBUG
    query.Print();
#endif
    
    

    //if control is here then no matching record was found
    return RET_FAILURE;
}

// Create <table_name>.meta.data file
// And write total pages used for table loading in it
void Sorted::WriteMetaData()
{
   if (!m_pFile->GetBinFilePath().empty())
   {
        ofstream meta_out;
        meta_out.open(string(m_pFile->GetBinFilePath() + ".meta.data").c_str(), ios::trunc);
        meta_out << "sorted\n";
        meta_out << m_pSortInfo->myOrder->ToString();
//	meta_out << "write m_pSortInfo here\n";
        meta_out.close();
   }
}

string Sorted :: getusec()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    stringstream ss;
    ss << tv.tv_sec;
    ss << ".";
    ss << tv.tv_usec;
    return ss.str();
}