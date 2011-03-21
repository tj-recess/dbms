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
#ifdef _DEBUG
    int cnt = 0;
#endif
    while(param->inputFile->GetNext(rec, *(param->selectOp), *(param->literalRec)))
    {
#ifdef _DEBUG
        cnt++;
#endif
        param->outputPipe->Insert(&rec);
    }
#ifdef _DEBUG
    cout<<"SelectFile : inserted " << cnt << " recs in output Pipe"<<endl;
#endif
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

void Join::Run(Pipe& inPipeL, Pipe& inPipeR, Pipe& outPipe, CNF& selOp, Record& literal)
{
    pthread_create(&thread, NULL, DoOperation, (void*)new Params(&inPipeL, &inPipeR, &outPipe, &selOp, &literal, runLen));
}

void* Join::DoOperation(void* p)
{
    Params* param = (Params*)p;
    /*actual logic goes here
     * 1. we have to create BigQL for inPipeL and BigQR for inPipeR
     * 2. Try to Create OrderMakers (omL and omR) for both BigQL and BigQR using CNF (use the GetSortOrder method)
     * 3. If (omL != null && omR != null) then go ahead and merge these 2 BigQs.(after constructing them of course)
     * 4. else - join these bigQs using “block-nested loops join” -- what the heck is that ?
     * 5. Put the results into outPipe and shut it down once done.
     */
    OrderMaker omL, omR;
    param->selectOp->GetSortOrders(omL, omR);
    if(omL.numAtts > 0 && omR.numAtts > 0)
    {
#ifdef _DEBUG
        cout<<"RelOp omL: ";
        omL.Print();
        cout<<"RelOp omR: ";
        omR.Print();
#endif
        const int pipeSize = 100;
        Pipe outL(pipeSize), outR(pipeSize);
        BigQ bigqL(*(param->inputPipeL), outL, omL, param->runLen);
        BigQ bigR(*(param->inputPipeR), outR, omR, param->runLen);
        Record *recL = new Record();
        Record *recR = new Record();

        while(outL.Remove(recL) && outR.Remove(recR))
        {
#ifdef _DEBUG
            cout<<"got record from both BigQL and BigQR"<<endl;
            cout<<"recL = "<< recL->bits <<endl;
            cout<<"recR = "<< recR->bits <<endl;
#endif
            int numAttsToKeep = omL.numAtts + omR.numAtts;
            int attsToKeep[numAttsToKeep];
            for(int i = 0; i < omL.numAtts; i++)
                attsToKeep[i] = omL.whichAtts[i];
            for(int i = omL.numAtts; i < numAttsToKeep; i++)
                attsToKeep[i] = omR.whichAtts[i];

            Record joinResult;
            joinResult.MergeRecords(recL, recR, omL.numAtts, omR.numAtts, attsToKeep, numAttsToKeep, omL.numAtts);
            param->outputPipe->Insert(&joinResult);
        }

    }
    param->outputPipe->ShutDown();
}

void Join::WaitUntilDone () {
	 pthread_join (thread, NULL);
}

void Join::Use_n_Pages (int runlen)
{
    //not sure if this runLen should be halved as
    //2 BigQs will be constructed
    runLen = runlen;
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
	Attribute att = {(char*)"sum", Double};

	Schema sum_schema((char*)"tmp_sum_schema_file", // filename, not used
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

/* Input: inPipe = fetch input records from here
 *        outPipe = push project output here
 *        mySchema = Schema of the records coming in inPipe 
 */
int DuplicateRemoval::m_nRunLen = 10;

void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema)
{
    // Create thread to do the project operation
    pthread_create(&m_thread, NULL, &DoOperation,
                   (void*) new Params(&inPipe, &outPipe, &mySchema));
    return;
}

void * DuplicateRemoval::DoOperation(void * p)
{
    Params* param = (Params*)p;
    Record lastSeenRec, currentRec;
	OrderMaker sortOrder;
	int pipeSize = 100;

	// make sort order maker for bigQ
    int n = param->pSchema->GetNumAtts();
    Attribute *atts = param->pSchema->GetAtts();
	// loop through all of the attributes, and list as it is
	sortOrder.numAtts = n;
    for (int i = 0; i < n; i++)
    {
		sortOrder.whichAtts[i] = i;
		sortOrder.whichTypes[i] = atts[i].myType;
	}

	// create local outPipe
	Pipe localOutPipe(pipeSize);
	// start bigQ
   	BigQ B(*(param->inputPipe), localOutPipe, sortOrder, m_nRunLen);

	bool bLastSeenRecSet = false;
	ComparisonEngine ce;
	while (localOutPipe.Remove(&currentRec))
	{
		if (bLastSeenRecSet == false)
		{
			lastSeenRec = currentRec;
			bLastSeenRecSet = true;
		}

		// compare currentRec with lastSeenRec
		// if same, do nothing
		// if different, send currentRec to param->outputPipe
		//				 and update lastSeenRec
		if (ce.Compare(&lastSeenRec, &currentRec, &sortOrder) != 0)
		{
			lastSeenRec = currentRec;
			param->outputPipe->Insert(&currentRec);
		}
	}

    //Shut down the outpipe
	localOutPipe.ShutDown();
    param->outputPipe->ShutDown();
	
    delete param;
    param = NULL;
}

void DuplicateRemoval::Use_n_Pages(int n)
{
	// set runLen for bigQ as the number of pages allowed for internal use
	m_nRunLen = n;
}

void DuplicateRemoval::WaitUntilDone()
{
    // Block until thread is done
    pthread_join(m_thread, 0);
}

