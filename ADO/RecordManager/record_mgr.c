#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"


typedef struct RecordManager // data structure to make use of Record Manager.
{
	BM_PageHandle pageHandle;
	BM_BufferPool bufferPool;	
	RID recordID;
	Expr *condition; // stores total number of tuples in the table
	int countTuples;
	int freePage;
	int countScan;
} RecordManager;

const int MAX_NUMBER_OF_PAGES = 100;
const int ATTRIBUTE_SIZE = 15; // Size of the name of the attribute

RecordManager *rManager;

/*  FUNCTION NAME : initRecordManager
    DESCRIPTION   : To Initialize Record Manager */
extern RC initRecordManager (void *mgmtData)
{
	initStorageManager();
	return RC_OK;
}

/*  FUNCTION NAME : initRecordManager
    DESCRIPTION   : To shut down the Record Manager */
extern RC shutdownRecordManager ()
{
	rManager = NULL;
	free(rManager);
	return RC_OK;
}

/*  FUNCTION NAME : createTable
    DESCRIPTION   : To create a TABLE with table name "name" and schema specified by "schema"  */
                
extern RC createTable (char *name, Schema *schema)
{
	
	rManager = (RecordManager*) malloc(sizeof(RecordManager)); // Allocate memory space to the record manager data structure
	initBufferPool(&rManager->bufferPool, name, MAX_NUMBER_OF_PAGES, RS_LRU, NULL); // Initalize Buffer Pool using LFU page replacement policy
    
	char info[PAGE_SIZE];
	char *pageHandle = info; 
	int result, k;
	*(int*)pageHandle = 0;  // Intializing number of tuples to 0
	pageHandle = pageHandle + sizeof(int);
	*(int*)pageHandle = 1;  // Intializing first page to one
	pageHandle = pageHandle + sizeof(int);
	*(int*)pageHandle = schema->numAttr;
	pageHandle = pageHandle + sizeof(int); 
	*(int*)pageHandle = schema->keySize;
	pageHandle = pageHandle + sizeof(int);
	for(k = 0; k < schema->numAttr; k++)
    {
       	strncpy(pageHandle, schema->attrNames[k], ATTRIBUTE_SIZE);
	    pageHandle = pageHandle + ATTRIBUTE_SIZE;
	    *(int*)pageHandle = (int)schema->dataTypes[k];
	    pageHandle = pageHandle + sizeof(int);
	    *(int*)pageHandle = (int) schema->typeLength[k];
	    pageHandle = pageHandle + sizeof(int);
    }
	SM_FileHandle fileHandle;
	if((result = createPageFile(name)) != RC_OK) // Create a page file name as table name using storage manager
		return result;
	if((result = openPageFile(name, &fileHandle)) != RC_OK) // Open the newly created page
		return result;
	if((result = writeBlock(0, &fileHandle, info)) != RC_OK) // Writing the schema to first location of the page file
		return result;
	if((result = closePageFile(&fileHandle)) != RC_OK) // Close the file after writing
		return result;
	return RC_OK;
}

/*  FUNCTION NAME : openTable
    DESCRIPTION   : To open the table with table name "name"  */

extern RC openTable (RM_TableData *rel, char *name)
{
	SM_PageHandle pageHandle;    
	int countAttributes, k;
	rel->mgmtData = rManager; // Setting table's meta data to record manager meta data structure
	rel->name = name; // Setting the table's name
	pinPage(&rManager->bufferPool, &rManager->pageHandle, 0); //Pinning a page
	pageHandle = (char*) rManager->pageHandle.data;
	rManager->countTuples= *(int*)pageHandle;
	pageHandle = pageHandle + sizeof(int);
	rManager->freePage= *(int*) pageHandle;
    pageHandle = pageHandle + sizeof(int);
    countAttributes = *(int*)pageHandle;
	pageHandle = pageHandle + sizeof(int);
	Schema *schema;
	schema = (Schema*) malloc(sizeof(Schema)); //Allocating memory space to 'schema'
	schema->numAttr = countAttributes;
	schema->attrNames = (char**) malloc(sizeof(char*) *countAttributes);
	schema->dataTypes = (DataType*) malloc(sizeof(DataType) *countAttributes);
	schema->typeLength = (int*) malloc(sizeof(int) *countAttributes);
	for(k = 0; k < countAttributes; k++) // Allocate memory space for storing attribute name for each attribute
		schema->attrNames[k]= (char*) malloc(ATTRIBUTE_SIZE);
	for(k = 0; k < schema->numAttr; k++)
    {
		strncpy(schema->attrNames[k], pageHandle, ATTRIBUTE_SIZE);
		pageHandle = pageHandle + ATTRIBUTE_SIZE;
		schema->dataTypes[k]= *(int*) pageHandle;
		pageHandle = pageHandle + sizeof(int);
		schema->typeLength[k]= *(int*)pageHandle;
		pageHandle = pageHandle + sizeof(int);
	}
	rel->schema = schema; //Initialising  newly created schema to the table's schema
	unpinPage(&rManager->bufferPool, &rManager->pageHandle); // Unpinning the page
	forcePage(&rManager->bufferPool, &rManager->pageHandle);
	return RC_OK;
}   
  
/*  FUNCTION NAME : closeTable
    DESCRIPTION   : To close the table as pointed by the parameter 'rel'  */

extern RC closeTable (RM_TableData *rel)
{
	RecordManager *rManager = rel->mgmtData; // Store the table's meta data
	shutdownBufferPool(&rManager->bufferPool); // shutdown Buffer Pool
	return RC_OK;
}

/*  FUNCTION NAME : deleteTable
    DESCRIPTION   : deletes the table with name specified by the parameter 'name'  */

extern RC deleteTable (char *name)
{
	destroyPageFile(name); // Remove the page file from memory using storage manager
	return RC_OK;
}

int findFreeSlot(char *data, int recordSize)
{
	int i, totalSpace = PAGE_SIZE / recordSize; 

	for (i = 0; i < totalSpace; i++)
		if (data[i * recordSize] != '+')
			return i;
	return -1;
}

/*  FUNCTION NAME : getNumTuples
    DESCRIPTION   : It returns the number of tuples in the table referenced by parameter 'rel'  */

extern int getNumTuples (RM_TableData *rel)
{
	RecordManager *rManager = rel->mgmtData; // Access data structure's tuplesCount and return it
	return rManager->countTuples;
}

/*  FUNCTION NAME : RC insertRecord
    DESCRIPTION   : Inserts a record in the table and updates the 'record' parameter with the Record ID passed in the insertRecord() function  */


extern RC insertRecord (RM_TableData *rel, Record *record)
{
	RecordManager *rManager = rel->mgmtData;	// Retrieve meta data stored in the table
	RID *recordID = &record->id;  // Initialising the Record ID for this record
	char *info, *pointerToSlots;
	int recordSize = getRecordSize(rel->schema); // Getting the size in bytes needed to store on record for the given schema
	recordID->page = rManager->freePage;
	pinPage(&rManager->bufferPool, &rManager->pageHandle, recordID->page); // Pinning a page
	info = rManager->pageHandle.data;
	recordID->slot = findFreeSlot(info, recordSize); // getting free slot
	while(recordID->slot == -1)
	{
		unpinPage(&rManager->bufferPool, &rManager->pageHandle);	
		recordID->page++;
		pinPage(&rManager->bufferPool, &rManager->pageHandle, recordID->page);		
		info = rManager->pageHandle.data;
		recordID->slot = findFreeSlot(info, recordSize);
	}
	pointerToSlots = info;
	markDirty(&rManager->bufferPool, &rManager->pageHandle); // Mark page dirty to notify that the page was modified
	pointerToSlots = pointerToSlots + (recordID->slot * recordSize); // Calculating slot starting position
	*pointerToSlots = '+'; // Appending '+' as tombstone to indicate this is a new record and should be removed if space is lesss
	memcpy(++pointerToSlots, record->data + 1, recordSize - 1); // Copy the record's data to the memory location pointed by slotPointer
	unpinPage(&rManager->bufferPool, &rManager->pageHandle); // Unpinning a page
	rManager->countTuples++;
	pinPage(&rManager->bufferPool, &rManager->pageHandle, 0); // pin back the page
	return RC_OK;
}

/*  FUNCTION NAME : RC deleteRecord
    DESCRIPTION   : deletes a record having Record ID 'id' passed through the parameter from the table referenced by the parameter 'rel'.  */


extern RC deleteRecord (RM_TableData *rel, RID id)
{
	RecordManager *rManager = rel->mgmtData;
	pinPage(&rManager->bufferPool, &rManager->pageHandle, id.page);
	rManager->freePage = id.page;
	char *info = rManager->pageHandle.data;
	int recordSize = getRecordSize(rel->schema); // Getting the size of the record
	info = info + (id.slot * recordSize); // Setting data pointer to the specific slot of the record
	*info = '-'; // '-' is used for Tombstone mechanism. It denotes that the record is deleted
	markDirty(&rManager->bufferPool, &rManager->pageHandle); // Marking the page dirty
	unpinPage(&rManager->bufferPool, &rManager->pageHandle);
	return RC_OK;
}

/*  FUNCTION NAME : RC updateRecord
    DESCRIPTION   : updates a record referenced by the parameter "record" in the table referenced by the parameter "rel".  */

extern RC updateRecord (RM_TableData *rel, Record *record)
{	
	RecordManager *rManager = rel->mgmtData;
	pinPage(&rManager->bufferPool, &rManager->pageHandle, record->id.page);
	char *info;
	int recordSize = getRecordSize(rel->schema);
	RID id = record->id;
	info = rManager->pageHandle.data; // Getting record data's memory location and calculating the start position of the new data
	info = info + (id.slot * recordSize);
	*info = '+';
	memcpy(++info, record->data + 1, recordSize - 1 );
	markDirty(&rManager->bufferPool, &rManager->pageHandle);
	unpinPage(&rManager->bufferPool, &rManager->pageHandle);
	return RC_OK;	
}

/*  FUNCTION NAME : RC getRecord
    DESCRIPTION   : retrieves a record having Record ID "id" passed in the parameter in the table referenced by "rel" which is also passed in the parameter. The result record is stored in the location referenced by the parameter "record". */
 

extern RC getRecord (RM_TableData *rel, RID id, Record *record)
{
	RecordManager *rManager = rel->mgmtData;
	pinPage(&rManager->bufferPool, &rManager->pageHandle, id.page); // Pinning the page which has the record we want to retreive
	int recordSize = getRecordSize(rel->schema);
	char *dataPointer = rManager->pageHandle.data;
	dataPointer = dataPointer + (id.slot * recordSize);
	if(*dataPointer != '+')
	{
		return RC_RM_NO_TUPLE_WITH_GIVEN_RID; // Return error if no matching record for Record ID 'id' is found in the table
	}
	else
	{
		record->id = id;
		char *info = record->data; // Setting the pointer to data field of 'record' so that we can copy the data of the record
		memcpy(++info, dataPointer + 1, recordSize - 1);
	}
	unpinPage(&rManager->bufferPool, &rManager->pageHandle);
	return RC_OK;
}

/*  FUNCTION NAME : RC startScan
    DESCRIPTION   : starts a scan by getting data from the RM_ScanHandle data structure which is passed as an argument to startScan() function. */


extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
	if (cond == NULL) // Checking if scan condition (test expression) is present
	{
		return RC_SCAN_CONDITION_NOT_FOUND;
	}
	openTable(rel, "ScanTable"); // Open the table in memory
    RecordManager *scanManager;
	RecordManager *tableManager;
	scanManager = (RecordManager*) malloc(sizeof(RecordManager)); // Allocating memory to the scanManager
    scan->mgmtData = scanManager;
    scanManager->recordID.page = 1; // start scan from the first page
    scanManager->recordID.slot = 0; // start scan from the first slot
	scanManager->countScan = 0;
	scanManager->condition = cond; // Setting the scan condition
    tableManager = rel->mgmtData; // Setting the our meta data to the table's meta data
	tableManager->countTuples = ATTRIBUTE_SIZE; // Setting tuple count
	scan->rel= rel; // Setting the table which has to be scanned using the specified condition
	return RC_OK;
}

/*  FUNCTION NAME : RC next
    DESCRIPTION   : scans each record in the table and stores the result record (record satisfying the condition) in the location pointed by 'record'. */


extern RC next (RM_ScanHandle *scan, Record *record)
{
	
	RecordManager *scanManager = scan->mgmtData; // Initiliazing scan data
	RecordManager *tableManager = scan->rel->mgmtData;
    Schema *schema = scan->rel->schema;
	if (scanManager->condition == NULL) // Checking if scan condition (test expression) is present
	{
		return RC_SCAN_CONDITION_NOT_FOUND;
	}
	Value *result = (Value *) malloc(sizeof(Value));
	char *info;
	int recordSize = getRecordSize(schema);
	int totalSpace = PAGE_SIZE / recordSize;
	int countScan = scanManager->countScan;
	int countTuples = tableManager->countTuples;
	if (countTuples == 0) // Checking if the table contains tuples. If the tables doesn't have tuple, then return respective message code
		return RC_RM_NO_MORE_TUPLES;
	while(countScan <= countTuples)
	{  
		if (countScan <= 0) // If all the tuples have been scanned, execute this block
		{
			scanManager->recordID.page = 1;
			scanManager->recordID.slot = 0;
		}
		else
		{
			scanManager->recordID.slot++;
			if(scanManager->recordID.slot >= totalSpace) // If all the slots have been scanned execute this block
			{
				scanManager->recordID.slot = 0;
				scanManager->recordID.page++;
			}
		}
		pinPage(&tableManager->bufferPool, &scanManager->pageHandle, scanManager->recordID.page);			
		info = scanManager->pageHandle.data;
		info = info + (scanManager->recordID.slot * recordSize); // Calulate the data location from record's slot and record size
		record->id.page = scanManager->recordID.page;
		record->id.slot = scanManager->recordID.slot;
		char *dataPointer = record->data;
		*dataPointer = '-';
		memcpy(++dataPointer, info + 1, recordSize - 1);
		scanManager->countScan++;
		countScan++;
		evalExpr(record, schema, scanManager->condition, &result);  // Test the record for the specified condition (test expression)
		if(result->v.boolV == TRUE) // v.boolV is TRUE if the record satisfies the condition
		{
			unpinPage(&tableManager->bufferPool, &scanManager->pageHandle);		
			return RC_OK;
		}
	}
	unpinPage(&tableManager->bufferPool, &scanManager->pageHandle);
	scanManager->recordID.page = 1;
	scanManager->recordID.slot = 0;
	scanManager->countScan = 0;
	return RC_RM_NO_MORE_TUPLES;
}

/*  FUNCTION NAME : RC closeScan
    DESCRIPTION   : closes the scan operation */


extern RC closeScan (RM_ScanHandle *scan)
{
	RecordManager *scanManager = scan->mgmtData;
	RecordManager *rManager = scan->rel->mgmtData;
	if(scanManager->countScan > 0) // Check if scan was incomplete
	{
		unpinPage(&rManager->bufferPool, &scanManager->pageHandle);
		scanManager->countScan = 0; // Reset the Scan Manager's values
		scanManager->recordID.page = 1;
		scanManager->recordID.slot = 0;
	}
    scan->mgmtData = NULL; // De-allocate all the memory space allocated to the scans's meta data
    free(scan->mgmtData);  
	return RC_OK;
}

/*  FUNCTION NAME : getRecordSize
    DESCRIPTION   : returns the size of a record in the specified schema. */

extern int getRecordSize (Schema *schema)
{
	int size = 0, i;  // offset set to zero
	for(i = 0; i < schema->numAttr; i++)
	{
		switch(schema->dataTypes[i]) // Switch depending on DATA TYPE of the ATTRIBUTE
		{
			case DT_STRING:
				size = size + schema->typeLength[i];
				break;
			case DT_INT:
				size = size + sizeof(int);
				break;
			case DT_FLOAT:
				size = size + sizeof(float);
				break;
			case DT_BOOL:
				size = size + sizeof(bool);
				break;
		}
	}
	return ++size;
}

/*  FUNCTION NAME : createSchema
    DESCRIPTION   : create a new schema */

extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
	Schema *schema = (Schema *) malloc(sizeof(Schema));	 // Allocate memory space to schema
	schema->numAttr = numAttr;
	schema->attrNames = attrNames;
	schema->dataTypes = dataTypes;
	schema->typeLength = typeLength;
	schema->keySize = keySize;
	schema->keyAttrs = keys;
	return schema; 
}

/*  FUNCTION NAME : RC attrOffset
    DESCRIPTION   : sets the offset (in bytes) from initial position to the specified attribute of the record into the 'result' parameter passed through the function */
//sets the offset (in bytes) from initial position to the specified attribute of the record into the 'result' parameter passed through the function
RC attrOffset (Schema *schema, int attrNum, int *result)
{
	int i;
	*result = 1;
	for(i = 0; i < attrNum; i++)
	{
		switch (schema->dataTypes[i])
		{
			case DT_STRING:
				*result = *result + schema->typeLength[i];
				break;
			case DT_INT:
				*result = *result + sizeof(int);
				break;
			case DT_FLOAT:
				*result = *result + sizeof(float);
				break;
			case DT_BOOL:
				*result = *result + sizeof(bool);
				break;
		}
	}
	return RC_OK;
}

/*  FUNCTION NAME : RC freeSchema
    DESCRIPTION   : removes a schema from memory and de-allocates all the memory space allocated to the schema. */

extern RC freeSchema (Schema *schema)
{
	free(schema);
	return RC_OK;
}

/*  FUNCTION NAME : RC createRecord
    DESCRIPTION   : creates a new record in the schema passed by parameter 'schema' and passes the new record to the 'record' parameter in the createRecord() */


extern RC createRecord (Record **record, Schema *schema)
{
	Record *newRecord = (Record*) malloc(sizeof(Record)); // Allocate some memory space for the new record
	int recordSize = getRecordSize(schema);    
	newRecord->data= (char*) malloc(recordSize); // Allocate some memory space for the data of new record
	newRecord->id.page = newRecord->id.slot = -1;  // Setting page and slot position to -1
	char *dataPointer = newRecord->data;
	*dataPointer = '-';
	*(++dataPointer) = '\0'; // Append '\0' which means NULL in C to the record after tombstone ++
	*record = newRecord; // Set the newly created record to 'record' which passed as argument
	return RC_OK;
}

/*  FUNCTION NAME : RC freeRecord
    DESCRIPTION   : removes the record from the memory */

extern RC freeRecord (Record *record)
{
	free(record); // De-allocating memory space allocated to record and freeing up that space
	return RC_OK;
}

/*  FUNCTION NAME : RC getAttr
    DESCRIPTION   : retrieves an attribute from the given record in the specified schema. */

extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
	int offset = 0;
	attrOffset(schema, attrNum, &offset); // Getting the ofset value of attributes depending on the attribute number
	Value *attribute = (Value*) malloc(sizeof(Value));
	char *dataPointer = record->data;
	dataPointer = dataPointer + offset;
	schema->dataTypes[attrNum] = (attrNum == 1) ? 1 : schema->dataTypes[attrNum];
	switch(schema->dataTypes[attrNum]) // Retrieve attribute's value depending on attribute's data type
	{
		case DT_STRING:
		{
			int length = schema->typeLength[attrNum];
			attribute->v.stringV = (char *) malloc(length + 1);
			strncpy(attribute->v.stringV, dataPointer, length);
			attribute->v.stringV[length] = '\0';
			attribute->dt = DT_STRING;
      		break;
		}
		case DT_INT:
		{
			int value = 0;
			memcpy(&value, dataPointer, sizeof(int));
			attribute->v.intV = value;
			attribute->dt = DT_INT;
      			break;
		}
		case DT_FLOAT:
		{
	  		float value;
	  		memcpy(&value, dataPointer, sizeof(float));
	  		attribute->v.floatV = value;
			attribute->dt = DT_FLOAT;
			break;
		}
		case DT_BOOL:
		{
			bool value;
			memcpy(&value,dataPointer, sizeof(bool));
			attribute->v.boolV = value;
			attribute->dt = DT_BOOL;
      		break;
		}

		default:
			printf("No Serializer defined for this datatype \n");
			break;
	}
	*value = attribute;
	return RC_OK;
}

/*  FUNCTION NAME : RC setAttr
    DESCRIPTION   : sets the attribute value in the record in the specified schema.  */


extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
	int offset = 0;
	attrOffset(schema, attrNum, &offset); // Getting the ofset value of attributes depending on the attribute number
	char *dataPointer = record->data;
	dataPointer = dataPointer + offset; // Adding offset to the starting position
	switch(schema->dataTypes[attrNum])
	{
		case DT_STRING:
		{
			int length = schema->typeLength[attrNum];
			strncpy(dataPointer, value->v.stringV, length);
			dataPointer = dataPointer + schema->typeLength[attrNum];
		  	break;
		}
		case DT_INT:
		{
			*(int *) dataPointer = value->v.intV;	  
			dataPointer = dataPointer + sizeof(int);
		  	break;
		}
		case DT_FLOAT:
		{
			*(float *) dataPointer = value->v.floatV;
			dataPointer = dataPointer + sizeof(float);
			break;
		}
		case DT_BOOL:
		{
			*(bool *) dataPointer = value->v.boolV;
			dataPointer = dataPointer + sizeof(bool);
			break;
		}
		default:
			printf("Serializer not defined for the given datatype. \n");
			break;
	}			
	return RC_OK;
}
