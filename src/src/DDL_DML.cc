#include "DDL_DML.h"
#include <string.h>

using namespace std;

DDL_DML::DDL_DML() : m_sTabName(), m_bSorted(false), m_nNumAtts(0), 
					 m_nSortAtts(0), m_pSchema(NULL), m_pOrderMaker(NULL)
{}

void DDL_DML::CreateTable(string sTabName, vector<Attribute> & col_atts_vec, 
						  bool sorted_table, vector<string> * pSortColAttsVec)
{
	// assign values to member variable
	m_sTabName = sTabName;
	m_bSorted = sorted_table;
	m_nNumAtts = col_atts_vec.size();


	FILE * out = fopen ("catalog", "a");
	fprintf (out, "\nBEGIN\n%s\n%s.tbl", sTabName.c_str(), sTabName.c_str());
	for (int i = 0; i < m_nNumAtts; i++)
	{
		fprintf (out, "\n%s ", col_atts_vec[i].name);
		if (col_atts_vec[i].myType == Int)
			fprintf (out, "Int");
		if (col_atts_vec[i].myType == Double)
			fprintf (out, "Double");
		if (col_atts_vec[i].myType == String)
			fprintf (out, "String");
	}
	fprintf (out, "\nEND\n");
	fclose(out);

	m_pSchema = new Schema("catalog", (char*)sTabName.c_str());

	// Sorted file
	if (m_bSorted)
	{
		m_nSortAtts = pSortColAttsVec->size();
        // Make an OrderMaker and store it
        m_pOrderMaker = new OrderMaker();
        m_pOrderMaker->numAtts = m_nSortAtts;
	
		// Error check: make sure these column names are valid
		for (int i = 0; i < m_nSortAtts; i++)
		{
			char * sortedCol = (char*)pSortColAttsVec->at(i).c_str();
			int found = 0;
			found = m_pSchema->Find(sortedCol);
			if (found == -1)
			{
				cerr << "\nERROR! Sorted column doesn't match table column\n";
				return;
			}
			else
			{
				m_pOrderMaker->whichAtts[i] = found;	// number of this col in the schema
				m_pOrderMaker->whichTypes[i] = m_pSchema->FindType(sortedCol);
			}
		}
	}
}

/*
void DDL_DML::LoadTable()
{
	set file type - heap or sorted

	if heap:
	--------
    DBFile dbfile;
    cout << " DBFile will be created at " << rel->path () << endl;
    dbfile.Create (rel->path(), heap, NULL);

    char tbl_path[100]; // construct path of the tpch flat text file
    sprintf (tbl_path, "%s%s.tbl", tpch_dir, rel->name());
    cout << " tpch file will be loaded from " << tbl_path << endl;

    dbfile.Load (*(rel->schema ()), tbl_path);
    dbfile.Close ();

	if sorted:
	---------
    struct {OrderMaker *o; int l;} startup = {&o, runlen};

    DBFile dbfile;
    cout << "\n output to dbfile : " << rel->path () << endl;
    dbfile.Create (rel->path(), sorted, &startup);
    dbfile.Close ();
}

void DDL_DML::DropTable()
{
	delete related data from catalog
	remove .bin file
}
*/
