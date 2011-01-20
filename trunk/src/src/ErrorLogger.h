/*
 * ErrorLogger.h
 *
 *  Created on: Jan 19, 2011
 *      Author: arpit
 */

#ifndef ERRORLOGGER_H_
#define ERRORLOGGER_H_

#include <fstream>

using namespace std;

class ErrorLogger 
{
private:
	static ofstream fout;
	static ErrorLogger *el;
	ErrorLogger();
public:
	virtual ~ErrorLogger();
	void writeLog(const string& msg);
	static ErrorLogger* getErrorLogger();
};

#endif /* ERRORLOGGER_H_ */
