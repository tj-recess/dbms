
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

void PrintTableCombinations(struct TableList *tables)
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
	vector <string> vTableName;
	map <string, int>::iterator map_itr = table_map.begin();
	for (; map_itr != table_map.end(); map_itr++)
	{
		vTableName.push_back(map_itr->first);
	}

	// Step 3
	vector <string> vTable2Names;
	cout << "\n\n--- Table combinations ---\n";
	int len = vTableName.size();
	for (int i = 0; i < len; i++)
	{
		for (int j = i+1; j < len; j++)
		{
			cout << vTableName.at(i) << "." << vTableName.at(j) << endl;
			vTable2Names.push_back(vTableName.at(i) + "." + vTableName.at(j));
		}
	}
	cout << endl;
}

int main () {
    yyparse();

    //print FuncOperator
    PrintFuncOperator(finalFunction);

    //print Tables list
    PrintTableList(tables);

	PrintTableCombinations(tables);

    char *fileName = "Statistics.txt";
    Statistics::PrepareStatisticsFile(fileName);
    Statistics s;
    s.Read(fileName);
}


