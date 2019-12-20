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

extern void initStorageManager (void){
    pageFile = NULL;  //initializing file pointer 
}

/* FUNCTION NAME : createPageFile
   DESCRIPTION   : Creates a new page file in append mode and write to it */

extern RC createPageFile (char *fileName){ // Function to create a file
    pageFile = fopen(fileName,"w+");       //open the file in append mode
    if(pageFile == NULL)
        return RC_FILE_NOT_FOUND;          
    else{
        SM_PageHandle new_page = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));
        if (fwrite(new_page,sizeof(char),PAGE_SIZE,pageFile)<PAGE_SIZE) //write empty page into file
            printf("write failed \n");
        else
            printf("write succeeded \n");
        fclose(pageFile);
        free(new_page);
        return RC_OK;  
    }
}

/* FUNCTION NAME : openPageFile
   DESCRIPTION   : Opens the created file in read mode to store informantion */ 

extern RC openPageFile (char *fileName, SM_FileHandle *fHandle){
    pageFile = fopen(fileName,"r");
    if(pageFile == NULL){
        return RC_FILE_NOT_FOUND;
    } else{
        fHandle->fileName = fileName;
        fHandle->curPagePos = 0;
        fseek(pageFile,0L,SEEK_END);
        int size = ftell(pageFile);
        fHandle->totalNumPages = size/PAGE_SIZE;
        fHandle->mgmtInfo = pageFile;
        fclose(pageFile);
        return RC_OK;
    }
}

/* FUNCTION NAME : closePageFile
   DESCRIPTION   : closes the opened file */

extern RC closePageFile (SM_FileHandle *fHandle){
    if(pageFile != NULL)
        pageFile = NULL;
    return RC_OK;
}

/* FUNCTION NAME : destroyPageFile 
   DESCRIPTION   : Deletes the page file  */

extern RC destroyPageFile (char *fileName){
    pageFile = fopen(fileName,"r");
    if(pageFile == NULL)
        return RC_FILE_NOT_FOUND;
    remove(fileName);
    return RC_OK;   
}

/* FUNCTION NAME : readBlock
   DESCRIPTION   : this function will read the pageNum-th block of data */

extern RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){

    if(pageNum > fHandle->totalNumPages && pageNum < 0) //Check if a page number is valid
        return RC_READ_NON_EXISTING_PAGE;               // return error message 
    pageFile = fopen(fHandle->fileName,"r");
    if (pageFile == NULL)
        return RC_FILE_NOT_FOUND;
    int read = fseek(pageFile,(pageNum*PAGE_SIZE),SEEK_SET); 
    if(read == 0)
        fread(memPage,sizeof(char),PAGE_SIZE,pageFile);
    else
        return RC_FILE_NOT_FOUND;
    fHandle->curPagePos = ftell(pageFile);  //move the pointer to current page position     
    fclose(pageFile);
    return RC_OK;   //return successful read

}

/* FUNCTION NAME : writeBlock 
   DESCRIPTION   : Writes the pageNum-th block of data */


extern RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
    if(pageNum > fHandle->totalNumPages || pageNum < 0) //Check if a page number is valid 
        return RC_WRITE_FAILED;
    pageFile = fopen(fHandle->fileName,"r+"); //open in write mode
    if(pageFile == NULL)
        return RC_FILE_NOT_FOUND;
    int write1 = fseek(pageFile,(pageNum*PAGE_SIZE), SEEK_SET);
    if(write1 == 0){
        fwrite(memPage,sizeof(char),strlen(memPage),pageFile);
        fHandle->curPagePos = ftell(pageFile);
        fclose(pageFile);
        return RC_OK;   
    }else
    {
        return RC_WRITE_FAILED;
    }
    
}

/* FUNCTION NAME : getBlockPos
   DESCRIPTION   : this function returns the current block position */

extern RC getBlockPos (SM_FileHandle *fHandle){
    int cur_pos;
    cur_pos = fHandle->curPagePos;
    return cur_pos;
}

/* FUNCTION NAME : readFirstBlock
   DESCRIPTION   : reads the first block of data from page file */

extern RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    pageFile = fopen(fHandle->fileName,"r");
    if(pageFile == NULL)
        return RC_FILE_NOT_FOUND;
    readBlock(0,fHandle,memPage);
    fclose(pageFile);
    return RC_OK;  
}

/* FUNCTION NAME : readPreviousBlock
   DESCRIPTION   : reads the page from previous block */

extern RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    //int prevPosition = fHandle->curPagePos/PAGE_SIZE;
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

extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    int currPosition = getBlockPos(fHandle);
    readBlock(currPosition, fHandle,memPage);
    return RC_OK; 
}

/* FUNCTION NAME : readNextBlock
   DESCRIPTION   : reads the page from next block */

extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    // int nextPosition = fHandle->curPagePos/PAGE_SIZE;
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
    //int lastPosition = fHandle->curPagePos/PAGE_SIZE;
    int lastPosition = fHandle->totalNumPages;
    if (fHandle->mgmtInfo ==  NULL)
        return RC_FILE_NOT_FOUND;
    readBlock(lastPosition, fHandle,memPage);
    return RC_OK;
}

/* FUNCTION NAME : writeCurrentBlock
   DESCRIPTION   : writes page to the current block */

extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    int curPosition = getBlockPos(fHandle);
    fHandle->totalNumPages++;
    writeBlock(curPosition,fHandle,memPage);
    return RC_OK;
}

/* FUNCTION NAME : appendEmptyBlock
   DESCRIPTION   : write an empty page to the file by appending at the end */

extern RC appendEmptyBlock (SM_FileHandle *fHandle){
  SM_PageHandle emptyBlock = (SM_PageHandle) calloc (PAGE_SIZE, sizeof(char));
  int seek = fseek(pageFile, 0, SEEK_END);
  if(seek == 0){
      fwrite(emptyBlock, sizeof(char),PAGE_SIZE, pageFile);
      printf("appended empty block \n");
  }
  else{
      free(emptyBlock);
      return RC_WRITE_FAILED;
  }
  fHandle->totalNumPages++;
  return RC_OK;

}

/* FUNCTION NAME : ensureCapacity
   DESCRIPTION   : if the file has less number of pages than totalNumPages then increase the size */

extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle){
    pageFile = fopen(fHandle->fileName, "a");
    if(pageFile == NULL)
        return RC_FILE_NOT_FOUND;
    while (numberOfPages > fHandle->totalNumPages)
    {
        appendEmptyBlock(fHandle);
    }
    fclose(pageFile);
    return RC_OK;   
}