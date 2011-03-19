#include "RelOp.h"

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal)
{
    pthread_create(&m_thread, NULL, DoOperation, 
				   (void*)new Params(&inFile, &outPipe, &selOp, &literal));
}

/*Logic:
 * Keep scanning the file using GetNext(..) and apply CNF (within GetNext(..),
 * insert into output pipe whichever record matches.
 * Shutdown the output Pipe
 */
void* SelectFile::DoOperation(void* p)
{
    Params* param = (Params*)p;
    param->inputFile->MoveFirst();
    Record rec;
    while(param->inputFile->GetNext(rec, *(param->selectOp), *(param->literalRec)))
    {
        param->outputPipe->Insert(&rec);
    }
    param->outputPipe->ShutDown();
	delete param;
	param = NULL;
}

void SelectFile::WaitUntilDone () {
	 pthread_join (m_thread, NULL);
}

void SelectFile::Use_n_Pages (int runlen)
{
    /*Arpit - This method has no impact in this class, probably.
     we have it just because it's pure virtual in
     */
}


/* Input: inPipe = fetch input records from here
 *	      outPipe = push project output here
 *        keepMe = array containing the attributes to project eg [3,5,2,7]
 *        numAttsInput = Total atts in input rec
 *        numAttsOutput = Atts to keep in output rec 
 *                        i.e. length of int * keepMe array
 */
void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, 
				   int numAttsInput, int numAttsOutput)
{
	// Create thread to do the project operation
	pthread_create(&m_thread, NULL, &DoOperation, 
				   (void*) new Params(&inPipe, &outPipe, keepMe, numAttsInput, numAttsOutput));
	
	return;
}

void * Project::DoOperation(void * p)
{
	Params* param = (Params*)p;
	Record rec;	
	// While records are coming from inPipe, 
	// modify records and keep only desired attributes
	// and push the modified records into outPipe
	while(param->inputPipe->Remove(&rec))
	{
		// Porject function will modify "rec" itself
		rec.Project(param->pAttsToKeep, param->numAttsToKeep, param->numAttsOriginal);
		// Push this modified rec in outPipe
		param->outputPipe->Insert(&rec);
	}
	
	//Shut down the outpipe
	param->outputPipe->ShutDown();
	delete param;
	param = NULL;
}

void Project::WaitUntilDone()
{
	// Block until thread is done
	pthread_join(m_thread, 0);
}


/* Input: inPipe = fetch input records from here
 * 		  outFile = File where text version should be written
 *					Its has been opened already
 *					TODO: caller doesn't close it, should we?
 *		  mySchema = the schema to use for writing records			
 */
void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema)
{
	// Create thread to do the project operation
	pthread_create(&m_thread, NULL, &DoOperation, 
				   (void*) new Params(&inPipe, &mySchema, outFile));
	
	return;
}

void * WriteOut::DoOperation(void * p)
{
	Params* param = (Params*)p;
	Record rec;	
	// While records are coming from inPipe, 
	// Write out the attributes in text form in outFile
	while(param->inputPipe->Remove(&rec))
	{
		// Basically copy the code from Record.cc (Record::Print function)
	    int n = param->pSchema->GetNumAtts();
	    Attribute *atts = param->pSchema->GetAtts();

	    // loop through all of the attributes
    	for (int i = 0; i < n; i++) 
		{

	        // print the attribute name
			fprintf(param->pFILE, "%s: ", atts[i].name);
    	    //cout << atts[i].name << ": ";

	        // use the i^th slot at the head of the record to get the
    	    // offset to the correct attribute in the record
        	int pointer = ((int *) rec.bits)[i + 1];

	        // here we determine the type, which given in the schema;
    	    // depending on the type we then print out the contents
			fprintf(param->pFILE, "[");
        	//cout << "[";

	        // first is integer
    	    if (atts[i].myType == Int) 
			{
        	    int *myInt = (int *) &(rec.bits[pointer]);
				fprintf(param->pFILE, "%d", *myInt);
            	//cout << *myInt;
		        // then is a double
        	} 
			else if (atts[i].myType == Double) 
			{
	            double *myDouble = (double *) &(rec.bits[pointer]);
				fprintf(param->pFILE, "%f", *myDouble);
    	        //cout << *myDouble;

        		// then is a character string
	        } 
			else if (atts[i].myType == String) 
			{
            	char *myString = (char *) &(rec.bits[pointer]);
				fprintf(param->pFILE, "%s", myString);
	            //cout << myString;
	        }
			fprintf(param->pFILE, "]");
	        //cout << "]";

    	    // print out a comma as needed to make things pretty
        	if (i != n - 1) 
			{
				fprintf(param->pFILE, ", ");
            	//cout << ", ";
	        }
    	}		// end of for loop
		fprintf(param->pFILE, "\n");
	    //cout << "\n";
	}			// no more records in the inPipe

	delete param;
	param = NULL;
}

void WriteOut::WaitUntilDone()
{
	// Block until thread is done
	pthread_join(m_thread, 0);
}


/* Input: inPipe = fetch input records from here
 *	      outPipe = push project output here
 *		  computeMe = Function using which sum must be computed
 */
void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe)
{
	// Create thread to do the project operation
	pthread_create(&m_thread, NULL, &DoOperation, 
				   (void*) new Params(&inPipe, &outPipe, &computeMe));
	
	return;
}

void * Sum::DoOperation(void * p)
{
	Params* param = (Params*)p;
	Record rec;	
	double sum = 0;
	// While records are coming from inPipe, 
	// Use function on them and calculate sum
	while(param->inputPipe->Remove(&rec))
	{
		int ival = 0; double dval = 0;
		param->computeFunc->Apply(rec, ival, dval);
		sum += (ival + dval);
	}

	// create temperory schema, with one attribute - sum
	Attribute att = {"sum", Double};

	Schema sum_schema("tmp_sum_schema_file", // filename, not used
					   1, 					 // number of attributes	
				       &att);				 // attribute pointer

	// Make a file that contains this sum
	FILE * sum_file = fopen("tmp_sum_data_file", "w");
	fprintf(sum_file, "%f|", sum);
	fclose(sum_file);
	sum_file = fopen("tmp_sum_data_file", "r");
	// Make record using the above schema and data file
	rec.SuckNextRecord(&sum_schema, sum_file);
	fclose(sum_file);

	// Push this record to outPipe
	param->outputPipe->Insert(&rec);

    // Shut down the outpipe
    param->outputPipe->ShutDown();

	// delete file "tmp_sum_data_file"
	if(remove("tmp_sum_data_file") != 0)
        perror("\nError in removing tmp_sum_data_file!");

    delete param;
    param = NULL;
}

void Sum::WaitUntilDone()
{
    // Block until thread is done
    pthread_join(m_thread, 0);
}
