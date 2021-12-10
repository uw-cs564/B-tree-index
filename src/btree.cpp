/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"

#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "filescan.h"

//#define DEBUG

namespace badgerdb {

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string &relationName,
                       std::string &outIndexName,
                       BufMgr *bufMgrIn,
                       const int attrByteOffset,
                       const Datatype attrType) {
    // initialize variables
    this->attributeType = attrType;
    this->attrByteOffset = attrByteOffset;
    scanExecuting = false;
    bufMgr = bufMgrIn;
    leafOccupancy = INTARRAYLEAFSIZE;
    nodeOccupancy = INTARRAYNONLEAFSIZE;
    Page *metaPage;
    Page *rootPage;
    // check correct datatype
    if (attrType != INTEGER) {
        std::cout << "Incorrect data type.";
        return;
    }
    // Creating the index file name, taken from the project specification
    std ::ostringstream indexStr;
    indexStr << relationName << '.' << attrByteOffset;
    outIndexName = indexStr.str();
    // Since meta contains information about the first page, we have to get the header page
    // but also have to see if the file exists.
    try {
        file = new BlobFile(outIndexName, false);
        headerPageNum = file->getFirstPageNo();

        // After we get the first page, we use meta's info to compare with the given info to see if it matches.
        bufMgr->readPage(file, headerPageNum, metaPage);
        IndexMetaInfo *meta = (IndexMetaInfo *)metaPage;
        if (relationName != meta->relationName) throw BadIndexInfoException("Index doesn't exist.");
        if (attributeType != meta->attrType) throw BadIndexInfoException("Index doesn't exist.");
        if (attrByteOffset != meta->attrByteOffset) throw BadIndexInfoException("Index  doesn't exist.");

        // unpin headerPage because we are finished with it but didn't modify it.
        bufMgr->unPinPage(file, headerPageNum, false);

    } catch (FileNotFoundException) {  // This means file doesn't exist so create a file.

        file = new BlobFile(outIndexName, true);

        // create a meta page for the new index.
        bufMgr->allocPage(file, headerPageNum, metaPage);
        bufMgr->allocPage(file, rootPageNum, rootPage);
        IndexMetaInfo *meta = (IndexMetaInfo *)metaPage;
        strncpy(meta->relationName, relationName.c_str(), 20);
        meta->attrType = attrType;
        meta->rootPageNo = rootPageNum;

        // Create the root.
        bufMgr->allocPage(file, rootPageNum, rootPage);
        NonLeafNodeInt *root = (NonLeafNodeInt *)rootPage;

        FileScan FS(relationName, bufMgr);
        // get every tuple from the relation and load into the new file.
        RecordId rid;
        while (true) {  // get all rids that meet the predicate and break if the end of the file of relationName is reached.
            try {
                // get
                FS.scanNext(rid);
                insertEntry(FS.getRecord().c_str() + attrByteOffset, rid);
            } catch (EndOfFileException EOFE) {
                break;
            }
        }
        // finished with the meta page and root page and we did modify it.
        bufMgr->unPinPage(file, headerPageNum, true);
        bufMgr->unPinPage(file, rootPageNum, true);
    }
    // bufMgrIn->readPage()
    // if (relationName != meta)
}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex() {
    // The destructor should end any scan, clear state variables, unpin pinned pages, and flush index file,
    // and deletes the file object.
    if (scanExecuting) {
        try {
            endScan();
        } catch (ScanNotInitializedException) {
            std::cout << "No scan has been initialized.";
        }
    }
    scanExecuting = false;
    bufMgr->flushFile(file);
    delete file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
    //
    if (rootNode) {
        //call insert
    }

    //case 2 - traverse tree till you find a node to insert into
    //if not full - insert into tree
    //else call split node
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertIntoNode
//this method is for when where is space within a node
//It finds the correct spot to insert and then inserts a node
// -----------------------------------------------------------------------------
void BTreeIndex::insertIntoNode(const int PageId, NonLeafNodeInt currNode, const void *key, const RecordId) {
    //insert end of the key array
    int size = currNode->keyArray.size;
    currNode->keyArray.add(*key);
    //for loop to traverse through the keyArray
    //so it is in sorted order 
    for (int i = 0; i < size; i++) {
        if (currNode->keyArray[size - i] < currNode->keyArray[size - i - 1])
            //swap key values
            int temp = currNode->keyArray[size - i - 1];
        currNode->keyArray[size - i - 1] = currNode->keyArray[size - i];
        currNode->keyArray[size - i] = temp;
        //swap rid values if it is a leaf node 
        if(currNode -> isNonLeaf){
            int tempRid = currNode->ridArray[size - i - 1];
            currNode->ridArray[size - i - 1] = currNode->keyArray[size - i];
            currNode->ridArray[size - i] = temp;
        }
        //decrement the availSpace
        currNode->availSpace--;
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertIntoLeafNode
//this method is for when inserting into a leaf - checks where it fits and insert
//calls split if nodes is full
//calls the insertintoNode method to sort it
// -----------------------------------------------------------------------------
void BTreeIndex::insertIntoLeafNode(const RecordId, const void *key, NonLeafNodeInt currNode) {
    //number of childNodes
    int numOfLeafNodes = currNode->pageNoArray.size;
    //read first node
    Page *insertPage;
    //look for a value less than final of a key array
    bool notFound = true;
    //keep looking till leaf node is selected to insert into
    while (notFound) {
        //for loop runs through all the children of a given Non Leaf Node
        for (int i = 0; i < numOfLeafNodes; i++) {
            //read the page
            bufMgr->readPage(this->file, currNode->pageNoArray[i], insertPage);
            //check if the key values is than the last value of the node
            if (*key < leafNode->keyArray[leafNode->keyArray.size - 1]) {
                //check that there is space to insert 
                if(currNode->spaceAvail > 0){
                    //call insert - to insert into correct postion of page
                    insertIntoNode(currNode->pageNoArray[i], currNode, *key, RecordId);
                }
                //if no room to insert into node, call split node 
                else {
                    splitLeafNode(*key, RecordId rid, currNode->pageNoArray[i])
                }
                //break out of for loop
                break;
                //break out of while loop
                notFound = false;
            }

            bufMgr->unPinPage(file, currNode->pageNoArray[i], true);
        }
    }

    // -----------------------------------------------------------------------------
    // BTreeIndex::create new Root
    //this method is for when to create a new Root node while propograting
    // -----------------------------------------------------------------------------
    void BTreeIndex::createNewRoot(const int PageId Page, const void *key, const RecordId rid, const PageId leftChild, const PageId rightChild, bool aboveLeaf) {
        //root page should always be 2
        PageId rootId = Page;
        Page *rootPage;
        //create non lead node for root
        NonLeafNodeInt rootNode;
        bufMgr->readPage(this->file, rootId, rootPage);
        //itialize new root node
        rootNode = (NonLeafNodeInt *)rootPage;
        //update new non leaf node
        if (aboveLeaf) {
            rootNode->level = 1;
        } else {
            rootNode->level = 0;
        }
        rootNode->keyArray.add(*key);
        //add left child
        rootNode->pageNoArray[0] = leftChild;
        //add right child
        rootNode->pageNoArray[1] = rightChild;
        rootNode->spaceAvail--;
        //unpin page
        this->bufMgr->unPinPage(this->file, rootId, true);
        //do we need to update the meta?
    }
    // -----------------------------------------------------------------------------
    // BTreeIndex::splitLeafNodes -  None Leaf nodes
    //called if spaceAvail = 0 when inserting
    // -----------------------------------------------------------------------------
    void BTreeIndex::splitNonLeafNode(const PageNo Page, NonLeafNodeInt currNode, const void *key, const RecordId rid, const PageId leftChild, const PageId rightChild) {
        //reads the page to split
        bufMgr->currNode.readPage(this->file, Page, currNode);
        //Create the new page(sibling)
        Page *newPage;
        PageId newPageId;
        //assign variables to sibling
        this->bufMgr->allocPage(this->file, newPageId, newPage);

        sibling->level = currNode->level;
        sibling->parentId = currNode->parentId;
        //insert the first two node values
        sibling->keyArray[0] = currNode->keyArray[0];
        sibling->keyArray[1] = currNode->keyArray[1];
        //assign the child page numbers to siblings
        sibling->pageNoArray[0] = currNode->pageNoArray[0];
        sibling->pageNoArray[1] = currNode->pageNoArray[1];
        //call insert into node
        insertIntoLeaf(currNode->parentPage, parent)
        //remove node?

        //assign children

        //reassign variables of the nodes

        //if parent page Id = 0 then call the newRootMethod
    }
    // -----------------------------------------------------------------------------
    // BTreeIndex::splitNode -  Leaf nodes
    // -----------------------------------------------------------------------------
    void BTreeIndex::splitLeafNode(const void *key, const RecordId rid, const PageNo Page) {
        //right biased
        //creates page of leaf node
        Page *leafPage;
        //reads the page to split
        bufMgr->readPage(this->file, Page, leafPage);
        //create node
        LeafNodeInt *curNode = (LeafNodeInt *)leafPage;
        //create new node to split into
        Page *newLeafPage;
        PageId newLeafPageId;
        //creates new page for split
        bufMgr->allocatePage(this->file, newLeafPageId, newLeafPage);
        //create a new node
        LeafNodeInt *splitNode = (LeafNodeInt *)newLeafPage;
        //split leafNode into size of node / 2, (size of node / 2)+1
        for (int i = 0; i < INTARRAYLEAFSIZE / 2; i++) {
            //sets the split node to the second half values of the current node
            splitNode->keyArray[i] = curNode->keyArray[INTARRAYLEAFSIZE / 2 + i];
            splitNode->ridArray[i] = curNode->ridArray[INTARRAYLEAFSIZE / 2 + i];
            //delete the values from the current Node after adding to split node
            curNode->keyArray[INTARRAYLEAFSIZE / 2 + i] = 0;
            curNode->ridArray[INTARRAYLEAFSIZE / 2 + i] = 0;
        }
        //update split node attributes
        splitNode->spaceAvail = INTARRAYLEAFSIZE - INTARRAYLEAFSIZE / 2;
        splitNode->rightSibPageNo = curNode->rightSibPageNo;
        splitNode->parentPage = curNode->parentPage
                                    //update all the attributes
                                    curNode->rightSibPageNo = newLeafPageId;
        curNode->spaceAvail = INTARRAYLEAFSIZE - INTARRAYLEAFSIZE / 2;
        //update parent node children array to contain splitNode pageID

        //if the key to insert is less than the final value of curNode
        if (curNode->keyArray[INTARRAYLEAFSIZE / 2 - 1] => *key) {
            //call insert for the curNode
            insertIntoLeafNode(RecordId, *key, NonLeafNodeInt curNode);
        }
        //else call insert for the splitNode
        else {
            insertIntoLeafNode(RecordId, *key, NonLeafNodeInt splitNode);
        }
        //push up the first index of the splitNode after insertion
        //tree is right biased

        //call insert into NonLeafNode

        //check if the above Node has space or call split Node
    }

    // -----------------------------------------------------------------------------
    // BTreeIndex::searchNode
    // -----------------------------------------------------------------------------
    void BTreeIndex::searchNode(const void *key, PageId &pid, PageId currentId, PageId parent) {
        // Reads the content of currentId into curPage
        Page *curPage;
        bufMgr->readPage(file, currentId, curPage);

        NonLeafNodeInt *curNode = (NonLeafNodeInt *)curPage;
    }

    // -----------------------------------------------------------------------------
    // BTreeIndex::startScan
    // -----------------------------------------------------------------------------

    void BTreeIndex::startScan(const void *lowValParm,
                               const Operator lowOpParm,
                               const void *highValParm,
                               const Operator highOpParm) {
        if ((lowOpParm != 'GT' && lowOpParm != 'GTE') ||
            (highOpParm != 'LT' && highOpParm != 'LTE')) {
            throw BadOpcodesException();
        }

        lowOp = lowOpParm;
        highOp = highOpParm;

        // Have to cast to int pointer first and then reference to get int value
        lowValInt = *(int *)lowValParm;
        highValInt = *(int *)highOpParm;

        // Make sure the parameter makes sense
        if (lowValInt > highValInt) throw BadScanrangeException();

        // only one scan at a time
        if (scanExecuting) endScan();

        /** ########### begin scan ########### */
        scanExecuting = true;
    }

    // -----------------------------------------------------------------------------
    // BTreeIndex::scanNext
    // -----------------------------------------------------------------------------

    void BTreeIndex::scanNext(RecordId & outRid) {
    }

    // -----------------------------------------------------------------------------
    // BTreeIndex::endScan
    // -----------------------------------------------------------------------------
    //
    void BTreeIndex::endScan() {
        if (!scanExecuting) {
            throw ScanNotInitializedException();
        }
        scanExecuting = false;
    }

}  // namespace badgerdb
