#include<stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
typedef struct RecordManager
{
	BM_PageHandle pageHandle;	
	BM_BufferPool bufferPool;
	RID recordID;
	Expr *condition;
	int tuplesCount; //stores total number of tuples in table
	int freePage; // stores the location of first free page which has an empty slot in table
	int scanCount; // it stores the count of number of records scanned
} RecordManager;

const int MAX_NUMBER_OF_PAGES = 100;
const int ATTRIBUTE_SIZE = 15;
RecordManager *rManager;

int findFreeSlot(char *data, int recordSize)
{
	int i, totalSlots = PAGE_SIZE / recordSize; 

	for (i = 0; i < totalSlots; i++)
		if (data[i * recordSize] != '+')
			return i;
	return -1;
}
extern RC initRecordManager (void *mgmtData){
	initStorageManager();
	return RC_OK;
}

extern RC shutdownRecordManager (){

	rManager = NULL;
	free(rManager);
	return RC_OK;
}

extern RC createTable (char *name, Schema *schema){
	rManager = (RecordManager*) malloc(sizeof(RecordManager));
	initBufferPool(&rManager->bufferPool, name, MAX_NUMBER_OF_PAGES, RS_LRU, NULL);
	char data[PAGE_SIZE];
	char *pageHandle = data;
	int result, k;
	*(int*)pageHandle = 0; 
	pageHandle = pageHandle + sizeof(int);
	*(int*)pageHandle = 1;
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
	if((result = createPageFile(name)) != RC_OK)
		return result;
	if((result = openPageFile(name, &fileHandle)) != RC_OK)
		return result;
	if((result = writeBlock(0, &fileHandle, data)) != RC_OK)
		return result;
	if((result = closePageFile(&fileHandle)) != RC_OK)
		return result;

	return RC_OK;
		
}

extern RC openTable (RM_TableData *rel, char *name)
{
	SM_PageHandle pageHandle;    
	
	int attributeCount, k;
	rel->mgmtData = rManager;
	rel->name = name;
	pinPage(&rManager->bufferPool, &rManager->pageHandle, 0);
	pageHandle = (char*) rManager->pageHandle.data;
	rManager->tuplesCount= *(int*)pageHandle;
	pageHandle = pageHandle + sizeof(int);

	// Getting free page from the page file
	rManager->freePage= *(int*) pageHandle;
    	pageHandle = pageHandle + sizeof(int);
	
	// Getting the number of attributes from the page file
    	attributeCount = *(int*)pageHandle;
	pageHandle = pageHandle + sizeof(int);
 	
	Schema *schema;

	// Allocating memory space to 'schema'
	schema = (Schema*) malloc(sizeof(Schema));
    
	// Setting schema's parameters
	schema->numAttr = attributeCount;
	schema->attrNames = (char**) malloc(sizeof(char*) *attributeCount);
	schema->dataTypes = (DataType*) malloc(sizeof(DataType) *attributeCount);
	schema->typeLength = (int*) malloc(sizeof(int) *attributeCount);

	// Allocate memory space for storing attribute name for each attribute
	for(k = 0; k < attributeCount; k++)
		schema->attrNames[k]= (char*) malloc(ATTRIBUTE_SIZE);
      
	for(k = 0; k < schema->numAttr; k++)
    	{
		// Setting attribute name
		strncpy(schema->attrNames[k], pageHandle, ATTRIBUTE_SIZE);
		pageHandle = pageHandle + ATTRIBUTE_SIZE;
	   
		// Setting data type of attribute
		schema->dataTypes[k]= *(int*) pageHandle;
		pageHandle = pageHandle + sizeof(int);

		// Setting length of datatype (length of STRING) of the attribute
		schema->typeLength[k]= *(int*)pageHandle;
		pageHandle = pageHandle + sizeof(int);
	}
	
	// Setting newly created schema to the table's schema
	rel->schema = schema;	

	// Unpinning the page i.e. removing it from Buffer Pool using BUffer Manager
	unpinPage(&rManager->bufferPool, &rManager->pageHandle);

	// Write the page back to disk using BUffer Manger
	forcePage(&rManager->bufferPool, &rManager->pageHandle);

	return RC_OK;
}   