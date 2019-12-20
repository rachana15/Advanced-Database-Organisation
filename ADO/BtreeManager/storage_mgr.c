#include<stdio.h>
#include<stdlib.h>
#include<sys/stat.h>
#include<sys/types.h>
#include<unistd.h>
#include<string.h>
#include<math.h>

#include "storage_mgr.h"

FILE *pageFile;

/*  FUNCTION NAME : initStorageManager
    DESCRIPTION   : Initialize the storage manager */

extern void initStorageManager (void) {
	pageFile = NULL;
}

/* FUNCTION NAME : createPageFile
   DESCRIPTION   : Creates a new page file in append mode and write to it */

extern RC createPageFile (char *fileName) {

	pageFile = fopen(fileName, "w+");
	if(pageFile == NULL) {
		return RC_FILE_NOT_FOUND;
	} else {
		
		SM_PageHandle newPage = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));
		if(fwrite(newPage, sizeof(char), PAGE_SIZE,pageFile) < PAGE_SIZE)
			printf("write failed \n");
		else
			printf("write succeeded \n");
		fclose(pageFile);
		free(newPage);
		return RC_OK;
	}
}

/* FUNCTION NAME : openPageFile
   DESCRIPTION   : Opens the created file in read mode to store informantion */ 

extern RC openPageFile (char *fileName, SM_FileHandle *fHandle) {
	
	pageFile = fopen(fileName, "r");
	if(pageFile == NULL) {
		return RC_FILE_NOT_FOUND;
	} else { 
		fHandle->fileName = fileName;
		fHandle->curPagePos = 0;
		struct stat fileInfo;
		if(fstat(fileno(pageFile), &fileInfo) < 0)    
			return RC_ERROR;
		fHandle->totalNumPages = fileInfo.st_size/ PAGE_SIZE;
		fclose(pageFile);
		return RC_OK;
	}
}

/* FUNCTION NAME : closePageFile
   DESCRIPTION   : closes the opened file */

extern RC closePageFile (SM_FileHandle *fHandle) {
	if(pageFile != NULL)
		pageFile = NULL;	
	return RC_OK; 
}

/* FUNCTION NAME : destroyPageFile 
   DESCRIPTION   : Deletes the page file  */

extern RC destroyPageFile (char *fileName) {	
	pageFile = fopen(fileName, "r");
	
	if(pageFile == NULL)
		return RC_FILE_NOT_FOUND; 
	remove(fileName);
	return RC_OK;
}

/* FUNCTION NAME : readBlock
   DESCRIPTION   : this function will read the pageNum-th block of data */

extern RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	if (pageNum > fHandle->totalNumPages || pageNum < 0)
        	return RC_READ_NON_EXISTING_PAGE;
	pageFile = fopen(fHandle->fileName, "r");
	if(pageFile == NULL)
		return RC_FILE_NOT_FOUND;
	int read = fseek(pageFile, (pageNum * PAGE_SIZE), SEEK_SET);
	if(read == 0) {
		if(fread(memPage, sizeof(char), PAGE_SIZE, pageFile) < PAGE_SIZE)
			return RC_ERROR;
	} else {
		return RC_READ_NON_EXISTING_PAGE; 
	}
	fHandle->curPagePos = ftell(pageFile); 	
	fclose(pageFile);
	return RC_OK;
}

/* FUNCTION NAME : getBlockPos
   DESCRIPTION   : this function returns the current block position */

extern int getBlockPos (SM_FileHandle *fHandle) {
	return fHandle->curPagePos;
}

/* FUNCTION NAME : readFirstBlock
   DESCRIPTION   : reads the first block of data from page file */

extern RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {	
	pageFile = fopen(fHandle->fileName, "r");
	if(pageFile == NULL)
		return RC_FILE_NOT_FOUND;
	
	readBlock(0,fHandle,memPage);
    fclose(pageFile);
    return RC_OK;  
}

/* FUNCTION NAME : readPreviousBlock
   DESCRIPTION   : reads the page from previous block */

extern RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
	 pageFile = fopen(fHandle->fileName,"r");
    if(pageFile == NULL)
        return RC_FILE_NOT_FOUND;
    int position = getBlockPos(fHandle);
    readBlock((position-1),fHandle,memPage);
    fclose(pageFile);
    return  RC_OK; 
}

/* FUNCTION NAME : readCurrentBlock
   DESCRIPTION   : reads the page from current block */

extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
	int currPosition = getBlockPos(fHandle);
    readBlock(currPosition, fHandle,memPage);
    return RC_OK; 		
}

/* FUNCTION NAME : readNextBlock
   DESCRIPTION   : reads the page from next block */

extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	 pageFile = fopen(fHandle->fileName,"r");
    if(pageFile == NULL)
        return RC_FILE_NOT_FOUND;
    int position = getBlockPos(fHandle);
    readBlock((position+1),fHandle,memPage);
    fclose(pageFile);
    return RC_OK; 
}

/* FUNCTION NAME : readLastBlock
   DESCRIPTION   :  read the page from last block */

extern RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	int lastPosition = fHandle->totalNumPages;
    if (fHandle->mgmtInfo ==  NULL)
        return RC_FILE_NOT_FOUND;
    readBlock(lastPosition, fHandle,memPage);
    return RC_OK;
}

/* FUNCTION NAME : writeBlock 
   DESCRIPTION   : Writes the pageNum-th block of data */

extern RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	if (pageNum > fHandle->totalNumPages || pageNum < 0)
        	return RC_WRITE_FAILED;
	pageFile = fopen(fHandle->fileName, "r+");
	if(pageFile == NULL)
		return RC_FILE_NOT_FOUND;
	int startPosition = pageNum * PAGE_SIZE;
	if(pageNum == 0) { 
		fseek(pageFile, startPosition, SEEK_SET);	
		int i;
		for(i = 0; i < PAGE_SIZE; i++) 
		{
			if(feof(pageFile)) 
				 appendEmptyBlock(fHandle);
			fputc(memPage[i], pageFile);
		}
		fHandle->curPagePos = ftell(pageFile); 
		fclose(pageFile);	
	} else {	
		fHandle->curPagePos = startPosition;
		fclose(pageFile);
		writeCurrentBlock(fHandle, memPage);
	}
	return RC_OK;
}

/* FUNCTION NAME : writeCurrentBlock
   DESCRIPTION   : writes page to the current block */


extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
	pageFile = fopen(fHandle->fileName, "r+");
	if(pageFile == NULL)
		return RC_FILE_NOT_FOUND;
	appendEmptyBlock(fHandle);
	fseek(pageFile, fHandle->curPagePos, SEEK_SET);
	fwrite(memPage, sizeof(char), strlen(memPage), pageFile);
	fHandle->curPagePos = ftell(pageFile);   	
	fclose(pageFile);
	return RC_OK;
}

/* FUNCTION NAME : appendEmptyBlock
   DESCRIPTION   : write an empty page to the file by appending at the end */

extern RC appendEmptyBlock (SM_FileHandle *fHandle) {
	SM_PageHandle emptyBlock = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));
	int isSeekSuccess = fseek(pageFile, 0, SEEK_END);
	if( isSeekSuccess == 0 ) {
		fwrite(emptyBlock, sizeof(char), PAGE_SIZE, pageFile);
	} else {
		free(emptyBlock);
		return RC_WRITE_FAILED;
	}
	free(emptyBlock);
	fHandle->totalNumPages++;
	return RC_OK;
}

/* FUNCTION NAME : ensureCapacity
   DESCRIPTION   : if the file has less number of pages than totalNumPages then increase the size */

extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle) {
	pageFile = fopen(fHandle->fileName, "a");
	if(pageFile == NULL)
		return RC_FILE_NOT_FOUND;
	while(numberOfPages > fHandle->totalNumPages)
		appendEmptyBlock(fHandle);
	fclose(pageFile);
	return RC_OK;
}