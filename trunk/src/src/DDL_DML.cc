#include "DDL_DML.h"
#include <string.h>

using namespace std;

DDL_DML::DDL_DML() : m_sTabName(), m_sTabType(), m_nNumAtts(0), m_aSortCols(NULL),
					 m_nSortAtts(0), m_pOrderMaker(NULL)
{}

void DDL_DML::CreateTable(string sTabName, Attribute* col_atts, 
						  int num_atts, string sTabType, 
						  char** sort_cols, int num_sort_atts)
{
	FILE * out = fopen ("catalog", "a");
	fprintf (out, "\nBEGIN\n%s\n%s.tbl", sTabName.c_str(), sTabName.c_str());
	for (int i = 0; i < num_atts; i++)
	{
		fprintf (out, "\n%s ", col_atts[i].name);
		if (col_atts[i].myType == Int)
			fprintf (out, "Int");
		if (col_atts[i].myType == Double)
			fprintf (out, "Double");
		if (col_atts[i].myType == String)
			fprintf (out, "String");
	}
	fprintf (out, "\nEND\n");
	fclose(out);

	// assign values to member variable
	m_sTabName = sTabName;
	m_sTabType = sTabType;
	m_nNumAtts = num_atts;
	m_aSortCols = sort_cols;

	// Sorted file
	if (sTabType.compare("SORTED") == 0)
	{
		if (sort_cols == NULL)
		{	
			cerr << "\nERROR! Sorted table needs columns on which it is sorted!\n";
			return;
		}
		
		// Error check: make sure these column names are valid
		for (int i = 0; i < num_sort_atts; i++)
		{
			bool found = false;
			for (int j = 0; j < num_atts; j++)
			{
				if (strcmp(sort_cols[i], col_atts[j].name) == 0)
				{
					found = true;
					break;
				}	
			}
			if (found == false)
			{
				cerr << "\nERROR! Sorted column doesn't match table column\n";
				return;
			}
		}

		// Then make an OrderMaker and store it
		// Use : get_sort_order() ?
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
