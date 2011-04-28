#ifndef DDL_DML_H
#define DDL_DML_H

#include "Schema.h"
#include <string>
#include <iostream>
#include <fstream>
#include <vector>
#include "DBFile.h"

class DDL_DML
{
	bool check_existing_table(string sTabName);
public:
	DDL_DML() {}
	~DDL_DML() {}
	void CreateTable(string sTabName, vector<Attribute> & col_atts_vec, 
					 bool sorted_table = false, vector<string> * sort_col_vec = NULL);
	void LoadTable(string sTabName, string sFileName);
	void DropTable(string sTabName);
};

#endif
