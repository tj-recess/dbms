
#include <iostream>
#include "ParseTree.h"
#include "Statistics.h"

using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

extern struct FuncOperator *finalFunction;
extern struct TableList *tables;

/*void PrintFuncOperator(struct FuncOperator *finalFunction)
{
        int i = 1;
        struct FuncOperator *temp;
        temp = finalFunction;
        while(temp != NULL)
        {
            cout << "Node = "<< i++ <<endl;
			cout << "\t[42: *, 43: +, 45: -, 47: /\n";
            cout << "\t Code = " << temp->code << endl;
            cout << "\t FuncOperand:" << endl;
            if(temp->leftOperand != NULL)
            {
                cout << "\t\t Code: " << temp->leftOperand->code << endl;
                cout << "\t\t Value: " << temp->leftOperand->value << endl;
            }
            else
                cout << "NULL" << endl;

            temp = temp->right;
        }

        if(finalFunction == NULL)
            cout << "finalFunction is null" <<endl;
}*/

void PrintFuncOperator(struct FuncOperator *finalFunction)
{
/* Structure of FuncOperator
   -------------------------

			NODE
			- code [0: leaf node, 42: *, 43: +, 45: -, 47: /]
			- leftOperand
				- code [1: double, 2: int, 3: name, 4: string, 5: less_than, 6: greater_than, 7: equals]
				- value
			- leftOperator (left child)
			- right (right child)

			example: (a.b + b)
	
			code: +
			leftOperand: NULL
			leftOperator		right
				/				  \
			   /				   \
		code: 0					code: 0
		leftOperand				leftOperand
			- name a.b				- name b
		leftOperator: NULL		leftOperator: NULL
		right: NULL				right: NULL			

*/
        static int pfo2_i = 1;
		if (finalFunction == NULL)
		{
			//cout << "finalFunction is null" <<endl;
			return;
		}

        struct FuncOperator *temp;
        temp = finalFunction;

        cout << "Node = "<< pfo2_i++ <<endl;
		// if code = 0, indicates it is a leaf node, otherwise as follows:
        cout << "\t[0: leaf node, 42: *, 43: +, 45: -, 47: / ]\n";
        cout << "\t Code = " << temp->code << endl;
        cout << "\t FuncOperand:" << endl;
        if(temp->leftOperand != NULL)
        {
			cout << "\t\t[1: double, 2: int, 3: name, 4: string, 5: less_than, 6: greater_than, 7: equals]\n";
            cout << "\t\t Code: " << temp->leftOperand->code << endl;
            cout << "\t\t Value: " << temp->leftOperand->value << endl;
        }
        else
            cout << "\t\tNULL" << endl;	// NULL indicates it has children

		PrintFuncOperator(temp->leftOperator);	// print left subtree
		PrintFuncOperator(temp->right);			// print right subtree
}

void PrintTableList(struct TableList *tables)
{
        struct TableList *tempTableList;
        tempTableList = tables;
        int i = 0;
        while(tempTableList != NULL)
        {
            cout << "Node = "<< i++ <<endl;
            cout << "\t Table Name: "<< tempTableList->tableName <<endl;
            cout << "\t Alias As: "<< tempTableList->aliasAs <<endl;


            tempTableList = tempTableList->next;
        }

        if(tables == NULL)
            cout << "tables specified is null" << endl;
}

// handles the base case when we have to find 2 table combos
void TableComboBaseCase(struct TableList *tables, vector <string> & vOrigTables,
							vector <string> & vTempCombos)
{
	/* Logic:
		1. Put table names in a map --> will get sorted automagically
		2. Take them out and put in a vector (easier to iterate over)
		3. Make combinations
	*/

	// Step 1
	struct TableList *p_TblList = tables;
	map <string, int> table_map;
	while (p_TblList != NULL)
	{
		table_map[p_TblList->tableName] = 1;
		p_TblList = p_TblList->next;
	}

	// Step 2
	map <string, int>::iterator map_itr = table_map.begin();
	for (; map_itr != table_map.end(); map_itr++)
	{
		vOrigTables.push_back(map_itr->first);
	}

	// Step 3
	cout << "\n\n--- Table combinations ---\n";
	int len = vOrigTables.size();
	for (int i = 0; i < len; i++)
	{
		for (int j = i+1; j < len; j++)
		{
			string sName = vOrigTables.at(i) + "." + vOrigTables.at(j);
			cout << sName.c_str() << endl;
			vTempCombos.push_back(sName);
		}
	}
	cout << endl;	
}

/* ABCDE

	2 combos:
	AB AC AD AE
	BC BD BE
	CD CE
	DE

	3 combos:
	A (find location after all A.x finish) --> BC BD BE CD CE DE
	B (find location after all B.x finish) --> CD CE DE
	C (find location after all C.x finish) --> DE
	D (find location after all D.x finish) --> none
	E (find location after all E.x finish) --> none

	==> ABC ABD ABE ACD ACE ADE BCD BCE BDE CDE

	4 combos: 
	A (find location after all A.x finish) --> BCD BCE BDE
	B (find location after all B.x finish) --> CDE
	C (find location after all C.x finish) --> none
	D (find location after all D.x finish) --> none
	E (find location after all E.x finish) --> none

*/

// fwd declaration
int find_after_loc(vector<string>&, string);

// recursive function to find all combinations of table
vector<string> PrintTableCombinations(struct TableList *tables, vector<string> & vOrigTables, int combo_len)
{
	vector <string> vTempCombos, vNewCombo;
	if (combo_len == 2)
	{
		TableComboBaseCase(tables, vOrigTables, vTempCombos);
		//cout << vOrigTables.size() << " " << vTempCombos.size() << endl;
		return vTempCombos;
	}
	else
	{
		vTempCombos = PrintTableCombinations(tables, vOrigTables, combo_len-1);
		//cout << vOrigTables.size() << " " << vTempCombos.size() << endl;
		int len = vOrigTables.size();
		int loc = 0;
		for (int i = 0; i <	len; i++)
		{
			loc = find_after_loc(vTempCombos, vOrigTables.at(i));
			// if lec = -1 --> error
			if (loc != -1)
			{
				for (int j = loc; j < vTempCombos.size(); j++)
				{
					string sName = vOrigTables.at(i) + "." + vTempCombos.at(j);
					cout << sName.c_str() << endl;
					vNewCombo.push_back(sName);
				}
			}
		}
		return vNewCombo;
	}
}

// Find the location where "sTableToFind" stops being the 1st table
// Logic: sTableToFind should appear first at least once, then stop
int find_after_loc(vector<string> & vTempCombos, string sTableToFind)
{
	// example: vTempCombos = A.B A.C A.D B.C B.D C.D
	// 		    sTableToFind = A, return 3
	//			sTableToFind = B, return 5
	int len = vTempCombos.size();
	int dotPos;
	bool bFoundOnce = false;
	for (int i = 0; i < len; i++)
	{
		string sTab = vTempCombos.at(i);
		dotPos = sTab.find(".");
		if (dotPos == string::npos)
			return -1;				// "." not found... error
		else
		{
			string sFirstTab = sTab.substr(0, dotPos);
			if (sFirstTab.compare(sTableToFind) == 0)
				bFoundOnce = true;
			else if (bFoundOnce == true)
				return i;
			else
				continue;
		}
	}
	return -1;
}

int main () 
{
    yyparse();

    //print FuncOperator
    PrintFuncOperator(finalFunction);

    //print Tables list
    PrintTableList(tables);

	vector<string> vOT;
	PrintTableCombinations(tables, vOT, 3);
    char *fileName = "Statistics.txt";
    Statistics::PrepareStatisticsFile(fileName);
    Statistics s;
    s.Read(fileName);
}


