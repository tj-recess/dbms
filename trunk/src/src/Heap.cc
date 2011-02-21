#include "Heap.h"

Heap::Heap()
{}

Heap::~Heap()
{ }

int Heap::Create(char *f_path, SortInfo* sortInfo)
{
    //ignore parameter sortInfo - not required for this file type
    GenericDBFile::Create(f_path, NULL);
}

int Heap::Open(char *fname)
{
    return GenericDBFile::Open(fname);
}

// returns 1 if successfully closed the file, 0 otherwise 
int Heap::Close()
{
    WriteMetaData();
    return GenericDBFile::Close();
}

/* Load function bulk loads the Heap instance from a text file, appending
 * new data to it using the SuckNextRecord function from Record.h. The character
 * string passed to Load is the name of the data file to bulk load.
 */
void Heap::Load (Schema &mySchema, char *loadMe)
{
    GenericDBFile::Load(mySchema, loadMe);
}

void Heap::Add (Record &rec, bool startFromNewPage)
{
    GenericDBFile::Add(rec, startFromNewPage);
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

