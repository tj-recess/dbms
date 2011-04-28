#include "DDL_DML.h"
#include <string.h>

using namespace std;

void DDL_DML::CreateTable(string sTabName, vector<Attribute> & col_atts_vec, 
						  bool bSortedTable, vector<string> * pSortColAttsVec)
{
	// assign values to member variable
	int nNumAtts = col_atts_vec.size();
	Schema * pSchema = NULL;
	DBFile DbFileObj;

	// Write this schema in the catalog file

	FILE * out = fopen ("catalog", "a");
	fprintf (out, "\nBEGIN\n%s\n%s.tbl", sTabName.c_str(), sTabName.c_str());
	for (int i = 0; i < nNumAtts; i++)
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

	// Fetch this schema into schema object
	pSchema = new Schema("catalog", (char*)sTabName.c_str());
	
	// Make binary file path
	string sBinOutput = sTabName + ".bin";

	// Sorted file
	if (bSortedTable)
	{
		int nSortAtts = pSortColAttsVec->size();
        // Make an OrderMaker and store it
        OrderMaker * pOrderMaker = new OrderMaker();
        pOrderMaker->numAtts = nSortAtts;
	
		// Error check: make sure these column names are valid
		for (int i = 0; i < nSortAtts; i++)
		{
			char * sortedCol = (char*)pSortColAttsVec->at(i).c_str();
			int found = 0;
			found = pSchema->Find(sortedCol);
			if (found == -1)
			{
				cerr << "\nERROR! Sorted column doesn't match table column\n";
				return;
			}
			else
			{
				pOrderMaker->whichAtts[i] = found;	// number of this col in the schema
				pOrderMaker->whichTypes[i] = pSchema->FindType(sortedCol);
			}
		}

		SortInfo sort_info_struct;
		sort_info_struct.myOrder = pOrderMaker;
		sort_info_struct.runLength = 50;

		DbFileObj.Create((char*)sBinOutput.c_str(), sorted, (void*)&sort_info_struct);	
	}
	else
	{
		// heap file
		DbFileObj.Create((char*)sBinOutput.c_str(), heap, NULL);
	}
	
	// Close the DB file
	DbFileObj.Close();
}

void DDL_DML::LoadTable(string sTabName, string sFileName)
{
	// Find current directory for the raw file
	char tbl_path[256];	// big enough to hold path
	getcwd(tbl_path, 256);
	string sRawFile = string(tbl_path) + "/" + sFileName;

	string sBinFile = sTabName + ".bin";

	// Open the file and then load it
	DBFile DbFileObj;
	DbFileObj.Open((char*)sBinFile.c_str());

	//Fetch schema and load the file
	Schema file_schema("catalog", (char*)sTabName.c_str());
	DbFileObj.Load(file_schema, (char*)sRawFile.c_str());
	DbFileObj.Close();
}
/*
void DDL_DML::DropTable()
{
	delete related data from catalog
	remove .bin file
}
*/
