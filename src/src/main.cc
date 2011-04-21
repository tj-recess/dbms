
#include <iostream>
#include "ParseTree.h"
#include "Statistics.h"

using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

extern struct FuncOperator *finalFunction;
extern struct TableList *tables;

void PrintFuncOperator(struct FuncOperator *finalFunction)
{
        int i = 1;
        struct FuncOperator *temp;
        temp = finalFunction;
        while(temp != NULL)
        {
            cout << "Node = "<< i++ <<endl;
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

int main () {
    yyparse();

    //print FuncOperator
    PrintFuncOperator(finalFunction);

    //print Tables list
    PrintTableList(tables);

    char *fileName = "Statistics.txt";
    Statistics::PrepareStatisticsFile(fileName);
    Statistics s;
    s.Read(fileName);
}


