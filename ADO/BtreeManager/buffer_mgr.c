#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <math.h>

// Struct Page represents a page frame in buffer pool
typedef struct Page
{
	SM_PageHandle info;  //used to store data 
	PageNumber pageNum; // identity for each page
	int dirtyBit; //page modification indicator
	int totalCount; // number of clients using a page at the given instance
	int hitNum;   // used for LRU replacement algorithm
} PageFrame;


int bufferCapacity = 0; // capacity of the buffer
int rearIndex = 0; // used by FIFO to calculate the front index
int writeCount = 0; // calculate number of pages written to disk
int hit = 0; // used by LRU to determine least recently added page into the buffer pool
int clockPointer = 0; // used by CLOCK replacement algorithm to point to the last added page

/*  FUNCTION NAME : initBufferPool
    DESCRIPTION   : This function creates a new buffer pool in memory. 
                    The parameter numPages defines the size of the buffer i.e. number of page frames that can be stored in the buffer. 
                    The pool is used to cache pages from the page file with name pageFileName. */

extern RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		  const int numPages, ReplacementStrategy strategy, 
		  void *stratinfo)
{
    
	bm->pageFile = (char *)pageFileName;
	bm->numPages = numPages;
	bm->strategy = strategy;
    // test to check for initialization of non existing buffer pool
    // if(strcmp(bm->pageFile, "testbuffer.bin") != 0){
    //     return RC_NON_EXISTING_PAGE;
    // }
	PageFrame *page = malloc(sizeof(PageFrame) * numPages);
	bufferCapacity = numPages;	//total number of pages in bufferpool
	int i;
	for(i = 0; i < bufferCapacity; i++)
	{
		page[i].info = NULL;
		page[i].pageNum = -1; 
		page[i].dirtyBit = 0;
		page[i].totalCount = 0;
		page[i].hitNum = 0;	
	}

	bm->mgmtData = page;
	writeCount   = 0; // initialiaze write count
    clockPointer = 0; // initialize Clock pointer
	return RC_OK;
		
}

/*  FUNCTION NAME : shutdownBufferPool
    DESCRIPTION   : This function first calls the forceFlushPool() which writes all the dirty pages to the disk.
                    Then it shuts down or close the buffer pool by removing all pages from memory
                    It releases all the memory allocated by setting the variables curQueueSize and curBufferSize to 0 */

extern RC shutdownBufferPool(BM_BufferPool *const bm)
{
	PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
	forceFlushPool(bm);
	int i;	
	for(i = 0; i < bufferCapacity; i++)
	{
		
		if(pageFrame[i].totalCount != 0) // content of page was modified but not written back to disk
		{
			return RC_PINNED_PAGES_IN_BUFFER;
		}
	}
	free(pageFrame);
	bm->mgmtData = NULL;
	return RC_OK;
}

/*  FUNCTION NAME : forceFlushPool
    DESCRIPTION   : causes all dirty pages (with fix count 0) from the buffer pool to be written to disk.
                    The written pages are now reset to "not dirty" i.e DirtyBit=0 and writeCount is incremented. */

extern RC forceFlushPool(BM_BufferPool *const bm)
{
	PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
	
	int i;	
	for(i = 0; i < bufferCapacity; i++)
	{
		if(pageFrame[i].totalCount == 0 && pageFrame[i].dirtyBit == 1)
		{
			SM_FileHandle fh;
			openPageFile(bm->pageFile, &fh);
			writeBlock(pageFrame[i].pageNum, &fh, pageFrame[i].info);
			pageFrame[i].dirtyBit = 0;
			writeCount++;
		}
	}	
	return RC_OK;
}

/*  FUNCTION NAME : markDirty
    DESCRIPTION   : This function marks the page dirty when modified
                    The page number in the buffer is found and its DirtyBit variable is set to 1. */

extern RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
	
	int i;
	for(i = 0; i < bufferCapacity; i++)
	{
		if(pageFrame[i].pageNum == page->pageNum)
		{
			pageFrame[i].dirtyBit = 1;
			return RC_OK;		
		}	
	}		
	return RC_ERROR;
}
/*  FUNCTION NAME : FIFO
    DESCRIPTION   : This replacement algorithm removes the first page frames arrived to the buffer pool */

extern void FIFO(BM_BufferPool *const bm, PageFrame *page)
{
	
	PageFrame *pageFrame = (PageFrame *) bm->mgmtData;
	
	int i, frontIndex;
	frontIndex = rearIndex % bufferCapacity; 
	for(i = 0; i < bufferCapacity; i++)
	{
		if(pageFrame[frontIndex].totalCount == 0)
		{
			
			if(pageFrame[frontIndex].dirtyBit == 1)
			{
				SM_FileHandle fh;
				openPageFile(bm->pageFile, &fh);
				writeBlock(pageFrame[frontIndex].pageNum, &fh, pageFrame[frontIndex].info);
				writeCount++;
			}
			pageFrame[frontIndex].info = page->info;
			pageFrame[frontIndex].pageNum = page->pageNum;
			pageFrame[frontIndex].dirtyBit = page->dirtyBit;
			pageFrame[frontIndex].totalCount = page->totalCount;
			break;
		}
		else
		{
			frontIndex++;
			frontIndex = (frontIndex % bufferCapacity == 0) ? 0 : frontIndex;
		}
	}
}

/*  FUNCTION NAME : LRU
    DESCRIPTION   : This algorithm replaces the least recently referenced page frames*/

extern void LRU(BM_BufferPool *const bm, PageFrame *page)
{	
	PageFrame *pageFrame = (PageFrame *) bm->mgmtData;
	int i, least_hit_index=0, least_hit_num;

	
	for(i = 0; i < bufferCapacity; i++)
	{
		
		if(pageFrame[i].totalCount == 0)
		{
			least_hit_index = i;
			least_hit_num = pageFrame[i].hitNum;
			break;
		}
	}	

	
	for(i = least_hit_index + 1; i < bufferCapacity; i++)
	{
		if(pageFrame[i].hitNum < least_hit_num)
		{
			least_hit_index = i;
			least_hit_num = pageFrame[i].hitNum;
		}
	}

	
	if(pageFrame[least_hit_index].dirtyBit == 1)
	{
		SM_FileHandle fh;
		openPageFile(bm->pageFile, &fh);
		writeBlock(pageFrame[least_hit_index].pageNum, &fh, pageFrame[least_hit_index].info);
		writeCount++;
	}
	pageFrame[least_hit_index].info = page->info;
	pageFrame[least_hit_index].pageNum = page->pageNum;
	pageFrame[least_hit_index].dirtyBit = page->dirtyBit;
	pageFrame[least_hit_index].totalCount = page->totalCount;
	pageFrame[least_hit_index].hitNum = page->hitNum;
}

/*  FUNCTION NAME : CLOCK
    DESCRIPTION   : It replaces the page that has been kept with Dirtybit = 0 for a long time */

extern void CLOCK(BM_BufferPool *const bm, PageFrame *page)
{	
	
	PageFrame *pageFrame = (PageFrame *) bm->mgmtData;
	while(1)
	{
		clockPointer = (clockPointer % bufferCapacity == 0) ? 0 : clockPointer;

		if(pageFrame[clockPointer].hitNum == 0)
		{
			
			if(pageFrame[clockPointer].dirtyBit == 1)
			{
				SM_FileHandle fh;
				openPageFile(bm->pageFile, &fh);
				writeBlock(pageFrame[clockPointer].pageNum, &fh, pageFrame[clockPointer].info);
				writeCount++;
			}
			
			// Setting page frame's content to new page's content
			pageFrame[clockPointer].info = page->info;
			pageFrame[clockPointer].pageNum = page->pageNum;
			pageFrame[clockPointer].dirtyBit = page->dirtyBit;
			pageFrame[clockPointer].totalCount = page->totalCount;
			pageFrame[clockPointer].hitNum = page->hitNum;
			clockPointer++;
			break;	
		}
		else
		{
			pageFrame[clockPointer++].hitNum = 0;		
		}
	}
}

/*  FUNCTION NAME : unpinPage
    DESCRIPTION   : This function loops through the PageFrames in BufferPool and finds the page to be unPinned.
                    Then it unpins the page page i.e. removes the page from memory.
                    pin status is set to 0 and the count variable TotalFix is decremented. */

extern RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{	
	PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
	
	int i;
    
	for(i = 0; i < bufferCapacity; i++)
	{
		if(pageFrame[i].pageNum == page->pageNum)
		{
			pageFrame[i].totalCount--;
			//break;
            return RC_OK;		
		}	
        
	}
	return RC_ERROR;
}

/*  FUNCTION NAME : forcePage
    DESCRIPTION   : This function writes the current content of the page back to the page file on disk.
                    The page file is opened and the contents are written to disk.
	                Then the write count is incremented and mark the page not dirty after updating the contents */

extern RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
	
	int i;
	
	for(i = 0; i < bufferCapacity; i++)
	{
		
		if(pageFrame[i].pageNum == page->pageNum)
		{		
			SM_FileHandle fh;
			openPageFile(bm->pageFile, &fh);
			writeBlock(pageFrame[i].pageNum, &fh, pageFrame[i].info);
			pageFrame[i].dirtyBit = 0;
			writeCount++;
            return RC_OK;
		}
	}	
	return RC_ERROR;
}

/*  FUNCTION NAME : getFrameContents
    DESCRIPTION   : This function returns the contents of page frame NO_PAGE constant is returned if there are no pages currently in the buffer.
	                It first checks if the buffer is full, if buffer is full it calls one of the replacement strategy. */

extern PageNumber *getFrameContents (BM_BufferPool *const bm)
{
	PageNumber *frameContents = malloc(sizeof(PageNumber) * bufferCapacity);
	PageFrame *pageFrame = (PageFrame *) bm->mgmtData;
	
	int i = 0;
	while(i < bufferCapacity) {
		frameContents[i] = (pageFrame[i].pageNum != -1) ? pageFrame[i].pageNum : NO_PAGE;
		i++;
	}
	return frameContents;
}

/*  FUNCTION NAME : getDirtyFlags
    DESCRIPTION   : We iterate over all the page frames in the buffer pool to get the dirtyBit value of the page frames present in the buffer pool.
	                Then it returns an arrays of boolean values representing the dirty status of the pages in the buffer. */

extern bool *getDirtyFlags (BM_BufferPool *const bm)
{
	bool *dirtyFlags = malloc(sizeof(bool) * bufferCapacity);
	PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
	
	int i;
	for(i = 0; i < bufferCapacity; i++)
	{
		dirtyFlags[i] = (pageFrame[i].dirtyBit == 1) ? true : false ;
	}	
	return dirtyFlags;
}

/*  FUNCTION NAME : getFixCounts
    DESCRIPTION   :  This function loops through all the frames created in BufferPool and returns the count variable TotalFix for each frame in BufferPool. */

extern int *getFixCounts (BM_BufferPool *const bm)
{
	int *totalCounts = malloc(sizeof(int) * bufferCapacity);
	PageFrame *pageFrame= (PageFrame *)bm->mgmtData;
	
	int i = 0;
	while(i < bufferCapacity)
	{
		totalCounts[i] = (pageFrame[i].totalCount != -1) ? pageFrame[i].totalCount : 0;
		i++;
	}	
	return totalCounts;
}

/*  FUNCTION NAME : getNumReadIO
    DESCRIPTION   : This function returns the number of pages that have been read from disk since a buffer pool has been initialized. */

extern int getNumReadIO (BM_BufferPool *const bm)
{
	return (rearIndex + 1);
}

/*  FUNCTION NAME : getNumWriteIO
    DESCRIPTION   : It returns the number of pages written to the page file since the buffer pool has been initialized. */

extern int getNumWriteIO (BM_BufferPool *const bm)
{
	return writeCount;
}

/*  FUNCTION NAME : pinPage
    DESCRIPTION   : This function pins the page with page number pageNum.
	                It first checks if the buffer is full, if buffer is full it calls one of the replacement strategy.
	                lse pages are read and pinned by using readBlock() and the count variable of the page TotalFix is incremented.*/

extern RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
	    const PageNumber pageNum)
{
	PageFrame *pageFrame = (PageFrame *)bm->mgmtData;

	// used to check if negative pages are getting pinned
	if(pageNum < -1){
        return RC_PIN_NEGATIVE_PAGE;
    }
	if(pageFrame[0].pageNum == -1)
	{
		
		SM_FileHandle fh;
		openPageFile(bm->pageFile, &fh);
		pageFrame[0].info = (SM_PageHandle) malloc(PAGE_SIZE);
        printf("\n negative page");
		ensureCapacity(pageNum,&fh);
		readBlock(pageNum, &fh, pageFrame[0].info);
		pageFrame[0].pageNum = pageNum;
		pageFrame[0].totalCount++;
		rearIndex = hit = 0;
		pageFrame[0].hitNum = hit;	
		page->pageNum = pageNum;
		page->data = pageFrame[0].info;
		
		return RC_OK;		
	}
	else
	{	
		int i;
		bool isBufferFull = true;
		
		for(i = 0; i < bufferCapacity; i++)
		{
			if(pageFrame[i].pageNum != -1)
			{	
				
				if(pageFrame[i].pageNum == pageNum)
				{
					
					pageFrame[i].totalCount++;
					isBufferFull = false;
					hit++; 

					if(bm->strategy == RS_LRU)	
						pageFrame[i].hitNum = hit;
                    else if(bm->strategy == RS_CLOCK)
						pageFrame[i].hitNum = 1;
					page->pageNum = pageNum;
					page->data = pageFrame[i].info;
                    clockPointer++;
					break;
				}				
			} else {
				SM_FileHandle fh;
				openPageFile(bm->pageFile, &fh);
				pageFrame[i].info = (SM_PageHandle) malloc(PAGE_SIZE);
				readBlock(pageNum, &fh, pageFrame[i].info);
				pageFrame[i].pageNum = pageNum;
				pageFrame[i].totalCount = 1;
				
				rearIndex++;	
				hit++; 
				if(bm->strategy == RS_LRU)
					pageFrame[i].hitNum = hit;
                else if(bm->strategy == RS_CLOCK)
						pageFrame[i].hitNum = 1;				
				page->pageNum = pageNum;
				page->data = pageFrame[i].info;
				
				isBufferFull = false;
				break;
			}
		}
		
		
		if(isBufferFull == true)
		{
			
			PageFrame *newPage = (PageFrame *) malloc(sizeof(PageFrame));		
			SM_FileHandle fh;
			openPageFile(bm->pageFile, &fh);
			newPage->info = (SM_PageHandle) malloc(PAGE_SIZE);
			readBlock(pageNum, &fh, newPage->info);
			newPage->pageNum = pageNum;
			newPage->dirtyBit = 0;		
			newPage->totalCount = 1;
			rearIndex++;
			hit++;

			if(bm->strategy == RS_LRU)
				newPage->hitNum = hit;	
            else if(bm->strategy == RS_CLOCK)
				pageFrame[i].hitNum = 1;		

			page->pageNum = pageNum;
			page->data = newPage->info;			

			switch(bm->strategy)
			{			
				case RS_FIFO: //FIFO algorithm
					FIFO(bm, newPage);
					break;
				
				case RS_LRU: // LRU algorithm
					LRU(bm, newPage);
					break;
				case RS_CLOCK:
                     CLOCK(bm, newPage);
                     break;
				default:
					printf("\n No Algorithm Implemented\n");
					break;
			}
						
		}		
		return RC_OK;
	}	
}